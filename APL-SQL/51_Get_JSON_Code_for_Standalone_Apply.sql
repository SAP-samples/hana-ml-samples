--- Output Table
drop table EXPORT_JSON;
create table EXPORT_JSON like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.RESULT";

DO BEGIN
    declare header "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
    declare config "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_DETAILED";   
    declare equation "SAP_PA_APL"."sap.pa.apl.base::BASE.T.RESULT";
    
    apl_model = select * from MODEL_BIN;   
    
    :config.insert(('APL/CodeType', 'JSON',null));
	
    "SAP_PA_APL"."sap.pa.apl.base::EXPORT_APPLY_CODE"(
	:header, :apl_model, :config, :equation);
	
	insert into EXPORT_JSON  select * from :equation;
END;

select to_char(VALUE) as "Scoring Equation" from "USER_APL"."EXPORT_JSON";