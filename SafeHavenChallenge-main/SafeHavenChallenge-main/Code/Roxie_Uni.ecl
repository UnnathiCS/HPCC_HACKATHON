IMPORT $,STD;
UpperIt(STRING txt) := Std.Str.ToUpperCase(txt);
//These INDEXes are created (built) in BWR_CleanChurches
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
CleanUniDS    := DATASET('~anonymous_solvers::Uni::file_Uni.ecl',CleanUniRec,FLAT);
CleanUniIDX     := INDEX(CleanUniDS,{city,state},{CleanUniDS},'~anonymous_solvers::idx::Uni::citypay');
CleanUniFIPSIDX := INDEX(CleanUniDS(PrimaryFIPS <> 0),{PrimaryFIPS},{CleanUniDS},'~anontmous_solvers::idx::Uni::fipspay');
/* To Publish your Query:
   1. Change Target to ROXIE
   2. Compile ONLY
   3. Open ECL Watch and select the Publish tab to publish your query 
   4. Test and demonstarte using: http://training.us-hpccsystems-dev.azure.lnrsg.io:8002
    
*/
EXPORT Roxie_Uni(FipsVal,STRING22 CityVal,STRING2 StateVal) := FUNCTION
 MyUni := IF(FipsVal = 0,
                OUTPUT(CleanUniIDX(City=UpperIt(CityVal),State=UpperIt(StateVal))),
                OUTPUT(CleanUniFIPSIDX(PrimaryFIPS=FipsVal)));
 RETURN MyUni;
END;


