--- Output Tables
drop table SCHEMA_OUT;
create COLUMN table SCHEMA_OUT like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.TABLE_TYPE";

drop table OP_LOG;
create COLUMN table OP_LOG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";

drop table APPLY_OUT;

drop table APPLY_LOG;
create column table APPLY_LOG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";

drop table SUMMARY;
create column table SUMMARY like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";

DO BEGIN
    declare header "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
    declare config "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_DETAILED";   
	declare schema_pred   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.TABLE_TYPE";     
    declare apl_log   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";      
    declare apl_sum   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";      

    :header.insert(('LogLevel', '8'));
	
	apl_model = select * from MODEL_BIN;
	
    :config.insert(('APL/ApplyExtraMode', 'Advanced Apply Settings',null));
    :config.insert(('APL/ApplyPredictedValue', 'true',null));
	:config.insert(('APL/ApplyContribution','all',null));
	
    "SAP_PA_APL"."sap.pa.apl.base::GET_TABLE_TYPE_FOR_APPLY"(
	:header, :apl_model, :config,  
	'APL_SAMPLES','AUTO_CLAIMS_FRAUD', 
	schema_pred, apl_log);

    "SAP_PA_APL"."sap.pa.apl.base::CREATE_TABLE_FROM_TABLE_TYPE"(
	'USER_APL','APPLY_OUT', :schema_pred);

    "SAP_PA_APL"."sap.pa.apl.base::APPLY_MODEL"(
	:header, :apl_model, :config, 
	'APL_SAMPLES','AUTO_CLAIMS_FRAUD', 
	'USER_APL','APPLY_OUT', apl_log, apl_sum);
	
	insert into  OP_LOG     select * from :apl_log;
	insert into  SUMMARY    select * from :apl_sum;
END;

select * from APPLY_OUT order by 2 desc;
