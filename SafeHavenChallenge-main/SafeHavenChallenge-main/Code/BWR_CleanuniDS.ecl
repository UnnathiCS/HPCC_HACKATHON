#OPTION('obfuscateOutput',True);
IMPORT $,STD;
//This file is used to demonstrate how to "clean" a raw dataset (Churches) and create an index to be used in a ROXIE service
Univesity := $.File_AllData.UniversityDS;
Cities := $.File_AllData.City_DS;


//First, determine what fields you want to clean:
CleanUniRec := RECORD
    STRING70 NAME;
    STRING70 ADDRESS;
    STRING22 CITY	;
    STRING2 STATE;
    STRING40 TELEPHONE;
    STRING22 COUNTY;
    STRING10  ZIP;
    STRING70  WEBSITE; 
    UNSIGNED3 PrimaryFIPS;
    END;
//PROJECT is used to transform one data record to another.
CleanUni := PROJECT(Univesity,TRANSFORM(CleanUniRec,
                                          SELF.NAME                := STD.STR.ToUpperCase(LEFT.NAME),
                                          SELF.TELEPHONE           := STD.STR.ToUpperCase(LEFT.TELEPHONE),
                                          SELF.COUNTY              := STD.STR.ToUpperCase(LEFT.COUNTY),
                                          SELF.ADDRESS             := STD.STR.ToUpperCase(LEFT.ADDRESS),
                                          SELF.CITY                := STD.STR.ToUpperCase(LEFT.CITY),
                                          SELF.STATE               := STD.STR.ToUpperCase(LEFT.STATE),
                                          SELF.WEBSITE            := LEFT.WEBSITE,
                                          SELF.ZIP                 := LEFT.ZIP,
                                          SELF.PrimaryFIPS         := 0
                                          ));
//JOIN is used to combine data from different datasets 
CleanUniFIPS :=       JOIN(CleanUni,Cities,
                           LEFT.city  = STD.STR.ToUpperCase(RIGHT.city) AND
                           LEFT.state = RIGHT.state_id,
                           TRANSFORM(CleanUniRec,
                                     SELF.PrimaryFIPS := (UNSIGNED3)RIGHT.county_fips,
                                     SELF             := LEFT),LEFT OUTER,LOOKUP);
//Write out the new file and then define it using DATASET
WriteUni     := OUTPUT(CleanUniFIPS,,'~anonymous_solvers::Uni::file_Uni.ecl',OVERWRITE,NAMED('CleanedUnis'));                                          
CleanUniDS    := DATASET('~anonymous_solvers::Uni::file_Uni.ecl',CleanUniRec,FLAT);

//Declare and Build Indexes (special datasets that can be used in the ROXIE data delivery cluster
CleanUniIDX     := INDEX(CleanUniDS,{city,state},{CleanUniDS},'~anonymous_solvers::idx::Uni::citypay');
CleanUniFIPSIDX := INDEX(CleanUniDS(PrimaryFIPS <> 0),{PrimaryFIPS},{CleanUniDS},'~anontmous_solvers::idx::Uni::fipspay');
BuildUniIDX     := BUILD(CleanUniIDX,OVERWRITE,NAMED('CityStateIDX'));
BuildUniFIPSIDX := BUILD(CleanUniFIPSIDX,OVERWRITE,NAMED('FIPSIDX'));

//SEQUENTIAL is similar to OUTPUT, but executes the actions in sequence instead of the default parallel actions of the HPCC
SEQUENTIAL(WriteUni,BuildUniIDX,BuildUniFIPSIDX);


