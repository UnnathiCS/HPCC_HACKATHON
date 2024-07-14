#OPTION('obfuscateOutput',TRUE);
IMPORT $,STD;
UpperIt(STRING txt) := Std.Str.ToUpperCase(txt);
//These INDEXes are created (built) in BWR_CleanChurches
CleanCampRec := RECORD
    STRING95  name;
    STRING50  dates_open;
    STRING50  phone;
    STRING50  amenities; 
    STRING35  city;
    STRING2   state;
    STRING35  nearest_town_bearing;
    UNSIGNED3 PrimaryFIPS;
END;
CleanCampDS    := DATASET('~anonymous_solvers::safe_camp::us_campsites.ecl',CleanCampRec,FLAT);
CleanCampIDX     := INDEX(CleanCampDS,{city,state},{CleanCampDS},'~anonymous_solvers::idx::hospital::citypay');
CleanCampFIPSIDX := INDEX(CleanCampDS(PrimaryFIPS <> 0),{PrimaryFIPS},{CleanCampDS},'~anontmous_solvers::idx::hospital::fipspay');
/* To Publish your Query:
   1. Change Target to ROXIE
   2. Compile ONLY
   3. Open ECL Watch and select the Publish tab to publish your query 
   4. Test and demonstarte using: http://training.us-hpccsystems-dev.azure.lnrsg.io:8002
    
*/
EXPORT Roxie_CampSites(FipsVal,STRING22 CityVal,STRING2 StateVal) := FUNCTION
 MyCamp := IF(FipsVal = 0,
                OUTPUT(CleanCampIDX(City=UpperIt(CityVal),State=UpperIt(StateVal))),
                OUTPUT(CleanCampFIPSIDX(PrimaryFIPS=FipsVal)));
 RETURN Mycamp;
END;


