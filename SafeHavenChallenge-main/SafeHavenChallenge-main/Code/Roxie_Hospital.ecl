IMPORT $,STD;
UpperIt(STRING txt) := Std.Str.ToUpperCase(txt);
//These INDEXes are created (built) in BWR_CleanChurches
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

CleanHospitalDS  := DATASET('~anonymous_solver::safe_hospitals::hospitals.ecl',CleanHospitalRec,FLAT);
CleanHospitalIDX     := INDEX(CleanHospitalDS,{city,state},{CleanHospitalDS},'~anonymous_solvers::idx::hospital::citypay');
CleanHospitalFIPSIDX := INDEX(CleanHospitalDS(PrimaryFIPS <> 0),{PrimaryFIPS},{CleanHospitalDS},'~anontmous_solvers::idx::hospital::fipspay');
/* To Publish your Query:
   1. Change Target to ROXIE
   2. Compile ONLY
   3. Open ECL Watch and select the Publish tab to publish your query 
   4. Test and demonstarte using: http://training.us-hpccsystems-dev.azure.lnrsg.io:8002
    
*/
EXPORT Roxie_Hospital(FipsVal,STRING22 CityVal,STRING2 StateVal) := FUNCTION
 MyHospital := IF(FipsVal = 0,
                OUTPUT(CleanHospitalIDX(City=UpperIt(CityVal),State=UpperIt(StateVal))),
                OUTPUT(CleanHospitalFIPSIDX(PrimaryFIPS=FipsVal)));
 RETURN MyHospital;
END;


