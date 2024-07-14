
IMPORT $,STD;
UpperIt(STRING txt) := Std.Str.ToUpperCase(txt);
//These INDEXes are created (built) in BWR_CleanChurches
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
CleanFoodDS      := DATASET('~anonymous_solvers::food::file_foodbank.ecl',CleanFoodRec,FLAT);
CleanFoodIDX     := INDEX(CleanFoodDS,{city,state},{CleanFoodDS},'~anonymous_solvers::idx::food::citypay');
CleanFoodFIPSIDX := INDEX(CleanFoodDS(PrimaryFIPS <> 0),{PrimaryFIPS},{CleanFoodDS},'~anontmous_solvers::idx::food::fipspay');
/* To Publish your Query:
   1. Change Target to ROXIE
   2. Compile ONLY
   3. Open ECL Watch and select the Publish tab to publish your query 
   4. Test and demonstarte using: http://training.us-hpccsystems-dev.azure.lnrsg.io:8002
    
*/
EXPORT Roxie_Food(FipsVal,STRING22 CityVal,STRING2 StateVal) := FUNCTION
 MyFood := IF(FipsVal = 0,
                OUTPUT(CleanFoodIDX(City=UpperIt(CityVal),State=UpperIt(StateVal))),
                OUTPUT(CleanFoodFIPSIDX(PrimaryFIPS=FipsVal)));
 RETURN MyFood;
END;


