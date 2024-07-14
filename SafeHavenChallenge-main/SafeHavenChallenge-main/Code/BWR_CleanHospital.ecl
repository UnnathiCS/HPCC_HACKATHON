//#OPTION('obfuscateOutput',True);
IMPORT $,STD;
//This file is used to demonstrate how to "clean" a raw dataset (Churches) and create an index to be used in a ROXIE service
Hospital := $.File_AllData.HospitalDS;
Cities := $.File_AllData.City_DS;


//First, determine what fields you want to clean:
CleanHospitalRec := RECORD
    STRING95  name;
    STRING6   status;
    STRING80  address;
    STRING20  type; 
    STRING35  city;
    STRING2   state;
    STRING10  zip;
    STRING15  telephone;
    STRING206 website;
    UNSIGNED3 PrimaryFIPS;
    END;
//PROJECT is used to transform one data record to another.
CleanHospital := PROJECT(Hospital,TRANSFORM(CleanHospitalRec,
                                          SELF.name                := STD.STR.ToUpperCase(LEFT.name),
                                          SELF.status              := STD.STR.ToUpperCase(LEFT.status),
                                          SELF.address             := STD.STR.ToUpperCase(LEFT.address),
                                          SELF.city                := STD.STR.ToUpperCase(LEFT.city),
                                          SELF.type                := STD.STR.ToUpperCase(LEFT.type),
                                          SELF.State               := STD.STR.ToUpperCase(LEFT.state),
                                          SELF.website             := LEFT.website,
                                          SELF.zip                 := LEFT.zip,
                                          SELF.telephone           := LEFT.telephone,
                                          SELF.PrimaryFIPS         := 0
                                          ));
//JOIN is used to combine data from different datasets 
CleanHospitalFIPS :=       JOIN(CleanHospital,Cities,
                           LEFT.city  = STD.STR.ToUpperCase(RIGHT.city) AND
                           LEFT.state = RIGHT.state_id,
                           TRANSFORM(CleanHospitalRec,
                                     SELF.PrimaryFIPS := (UNSIGNED3)RIGHT.county_fips,
                                     SELF             := LEFT),LEFT OUTER,LOOKUP);
//Write out the new file and then define it using DATASET
WriteHospital      := OUTPUT(CleanHospitalFIPS,,'~anonymous_solver::safe_hospitals::hospitals.ecl',OVERWRITE,NAMED('CleanedHospital'));                                          
CleanHospitalDS    := DATASET('~anonymous_solver::safe_hospitals::hospitals.ecl',CleanHospitalRec,FLAT);

//Declare and Build Indexes (special datasets that can be used in the ROXIE data delivery cluster
CleanHospitalIDX     := INDEX(CleanHospitalDS,{city,state},{CleanHospitalDS},'~anonymous_solvers::idx::hospital::citypay');
CleanHospitalFIPSIDX := INDEX(CleanHospitalDS(PrimaryFIPS <> 0),{PrimaryFIPS},{CleanHospitalDS},'~anontmous_solvers::idx::hospital::fipspay');
BuildHospitalIDX     := BUILD(CleanHospitalIDX,OVERWRITE,NAMED('CityStateIDX'));
BuildHospitalFIPSIDX := BUILD(CleanHospitalFIPSIDX,OVERWRITE,NAMED('FIPSIDX'));

//SEQUENTIAL is similar to OUTPUT, but executes the actions in sequence instead of the default parallel actions of the HPCC
SEQUENTIAL(WriteHospital,BuildHospitalIDX,BuildHospitalFIPSIDX);


