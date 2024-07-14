//#OPTION('obfuscateOutput',True);
IMPORT $,STD;
//This file is used to demonstrate how to "clean" a raw dataset (Churches) and create an index to be used in a ROXIE service
Food := $.File_AllData.FoodBankDS;
Cities := $.File_AllData.City_DS;


//First, determine what fields you want to clean:
CleanFoodRec := RECORD
    STRING70  food_bank_name;
    STRING35  status;
    UNSIGNED3 fema_region;
    STRING70  address;
    STRING22  city;
    STRING2   state;
    STRING10  zip;
    STRING70  web_page; 
    UNSIGNED3 PrimaryFIPS;
    END;
//PROJECT is used to transform one data record to another.
CleanFood := PROJECT(Food,TRANSFORM(CleanFoodRec,
                                          SELF.food_bank_name      := STD.STR.ToUpperCase(LEFT.food_bank_name),
                                          SELF.status              := STD.STR.ToUpperCase(LEFT.status),
                                          SELF.address             := STD.STR.ToUpperCase(LEFT.address),
                                          SELF.city                := STD.STR.ToUpperCase(LEFT.city),
                                          SELF.State               := STD.STR.ToUpperCase(LEFT.state),
                                          SELF.web_page            := LEFT.web_page,
                                          SELF.zip                 := LEFT. zip_code,
                                          SELF.fema_region         := LEFT.fema_region,
                                          SELF.PrimaryFIPS         := 0
                                          ));
//JOIN is used to combine data from different datasets 
CleanFoodFIPS :=       JOIN(CleanFood,Cities,
                           LEFT.city  = STD.STR.ToUpperCase(RIGHT.city) AND
                           LEFT.state = RIGHT.state_id,
                           TRANSFORM(CleanFoodRec,
                                     SELF.PrimaryFIPS := (UNSIGNED3)RIGHT.county_fips,
                                     SELF             := LEFT),LEFT OUTER,LOOKUP);
//Write out the new file and then define it using DATASET
WriteFood      := OUTPUT(CleanFoodFIPS,,'~anonymous_solvers::food::file_foodbank.ecl',OVERWRITE,NAMED('CleanedFood'));                                          
CleanFoodDS    := DATASET('~anonymous_solvers::food::file_foodbank.ecl',CleanFoodRec,FLAT);

//Declare and Build Indexes (special datasets that can be used in the ROXIE data delivery cluster
CleanFoodIDX     := INDEX(CleanFoodDS,{city,state},{CleanFoodDS},'~anonymous_solvers::idx::food::citypay');
CleanFoodFIPSIDX := INDEX(CleanFoodDS(PrimaryFIPS <> 0),{PrimaryFIPS},{CleanFoodDS},'~anontmous_solvers::idx::food::fipspay');
BuildFoodIDX     := BUILD(CleanFoodIDX,OVERWRITE,NAMED('CityStateIDX'));
BuildFoodFIPSIDX := BUILD(CleanFoodFIPSIDX,OVERWRITE,NAMED('FIPSIDX'));

//SEQUENTIAL is similar to OUTPUT, but executes the actions in sequence instead of the default parallel actions of the HPCC
SEQUENTIAL(WriteFood,BuildFoodIDX,BuildFoodFIPSIDX);


