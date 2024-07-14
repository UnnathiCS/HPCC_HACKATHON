//#OPTION('obfuscateOutput',True);
IMPORT $,STD;
//This file is used to demonstrate how to "clean" a raw dataset (Churches) and create an index to be used in a ROXIE service
CampSite:= $.File_AllData.CampDS;
Cities := $.File_AllData.City_DS;


//First, determine what fields you want to clean:
CleanCampRec := RECORD
    STRING95  name;
    STRING50  dates_open;
    STRING50  phone;
    STRING50 amenities; 
    STRING35  city;
    STRING2   state;
    STRING35  nearest_town_bearing;
    UNSIGNED3 PrimaryFIPS;
    END;
//PROJECT is used to transform one data record to another.
CleanCamp := PROJECT(CampSite,TRANSFORM(CleanCampRec,
                                          SELF.name                := STD.STR.ToUpperCase(LEFT.name),
                                          SELF.dates_open          := STD.STR.ToUpperCase(LEFT.dates_open),
                                          SELF.amenities           := STD.STR.ToUpperCase(LEFT.amenities),
                                          SELF.city                := STD.STR.ToUpperCase(LEFT.city),
                                          SELF.State               := STD.STR.ToUpperCase(LEFT.state),
                                          SELF.phone               := STD.STR.ToUpperCase(LEFT.phone),
                                          SELF.nearest_town_bearing:= STD.STR.ToUpperCase(LEFT.nearest_town_bearing),
                                          SELF.PrimaryFIPS :=0 
                                          ));
//JOIN is used to combine data from different datasets 
CleanCampFIPS :=       JOIN(CleanCamp,Cities,
                           LEFT.city  = STD.STR.ToUpperCase(RIGHT.city) AND
                           LEFT.state = RIGHT.state_id,
                           TRANSFORM(CleanCampRec,
                                     SELF.PrimaryFIPS := (UNSIGNED3)RIGHT.county_fips,
                                     SELF             := LEFT),LEFT OUTER,LOOKUP);
//Write out the new file and then define it using DATASET
WriteCamp     := OUTPUT(CleanCampFIPS,,'~anonymous_solvers::safe_camp::us_campsites.ecl',OVERWRITE,NAMED('CleanedCampSites'));                                          
CleanCampDS    := DATASET('~anonymous_solvers::safe_camp::us_campsites.ecl',CleanCampRec,FLAT);

//Declare and Build Indexes (special datasets that can be used in the ROXIE data delivery cluster
CleanCampIDX     := INDEX(CleanCampDS,{city,state},{CleanCampDS},'~anonymous_solvers::idx::hospital::citypay');
CleanCampFIPSIDX := INDEX(CleanCampDS(PrimaryFIPS <> 0),{PrimaryFIPS},{CleanCampDS},'~anontmous_solvers::idx::hospital::fipspay');
BuildCampIDX     := BUILD(CleanCampIDX,OVERWRITE,NAMED('CityStateIDX'));
BuildCampFIPSIDX := BUILD(CleanCampFIPSIDX,OVERWRITE,NAMED('FIPSIDX'));

//SEQUENTIAL is similar to OUTPUT, but executes the actions in sequence instead of the default parallel actions of the HPCC
SEQUENTIAL(WriteCamp,BuildCampIDX,BuildCampFIPSIDX);


