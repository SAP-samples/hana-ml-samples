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
    :config.insert(('APL/ApplyProbability', 'true',null));
    :config.insert(('APL/ApplyDecision', 'true',null));
    :config.insert(('APL/ApplyReasonCode/TopCount', '5',null));
	:config.insert(('APL/ApplyReasonCode/ShowStrengthValue', 'true',null));
    :config.insert(('APL/ApplyReasonCode/ShowStrengthIndicator', 'false',null));
    :config.insert(('APL/ApplyReasonCode/ShowOtherStrength', 'true',null));	
    :config.insert(('APL/ApplyReasonCode/RankOnAbsoluteValues', 'true',null));
	
    "SAP_PA_APL"."sap.pa.apl.base::GET_TABLE_TYPE_FOR_APPLY"(
	:header, :apl_model, :config,  
	'APL_SAMPLES','AUTO_CLAIMS_NEW', 
	schema_pred, apl_log);

    "SAP_PA_APL"."sap.pa.apl.base::CREATE_TABLE_FROM_TABLE_TYPE"(
	'USER_APL','APPLY_OUT', :schema_pred);

    "SAP_PA_APL"."sap.pa.apl.base::APPLY_MODEL"(
	:header, :apl_model, :config, 
	'APL_SAMPLES','AUTO_CLAIMS_NEW', 
	'USER_APL','APPLY_OUT', apl_log, apl_sum);
	
	insert into  OP_LOG     select * from :apl_log;
	insert into  SUMMARY    select * from :apl_sum;
END;

select 
 "CLAIM_ID" as "Claim Id", 
 round("gb_score_IS_FRAUD",2) as "Score", 
 "gb_decision_IS_FRAUD" as "Decision",
 round("gb_proba_IS_FRAUD",2) as "Probability",
 "gb_reason_top_1_name" as "Top 1 Name", "gb_reason_top_1_value" as "Top 1 Value",
 "gb_reason_top_2_name" as "Top 2 Name", "gb_reason_top_2_value" as "Top 2 Value",
 "gb_reason_top_3_name" as "Top 3 Name", "gb_reason_top_3_value" as "Top 3 Value",
 "gb_reason_top_4_name" as "Top 4 Name", "gb_reason_top_4_value" as "Top 4 Value",
 "gb_reason_top_5_name" as "Top 5 Name", "gb_reason_top_5_value" as "Top 5 Value"
from APPLY_OUT 
where "CLAIM_ID" = 'CL_1027985'
order by 2 desc;

select * from (
select 
 "CLAIM_ID" as "Claim Id",  1 as "Rank",
 "gb_reason_top_1_name" as "Variable", "gb_reason_top_1_value" as "Actual Value", 
 round("gb_reason_top_1_strength",2) as "Strength"
from APPLY_OUT where "CLAIM_ID" = 'CL_1027985'
UNION
select 
 "CLAIM_ID", 2, "gb_reason_top_2_name" , "gb_reason_top_2_value", round("gb_reason_top_2_strength",2) 
from APPLY_OUT where "CLAIM_ID" = 'CL_1027985'
UNION
select 
 "CLAIM_ID", 3, "gb_reason_top_3_name" , "gb_reason_top_3_value", round("gb_reason_top_3_strength",2) 
from APPLY_OUT where "CLAIM_ID" = 'CL_1027985'
UNION
select 
 "CLAIM_ID", 4, "gb_reason_top_4_name" , "gb_reason_top_4_value", round("gb_reason_top_4_strength",2) 
from APPLY_OUT where "CLAIM_ID" = 'CL_1027985'
UNION
select 
 "CLAIM_ID", 5, "gb_reason_top_5_name" , "gb_reason_top_5_value", round("gb_reason_top_5_strength",2) 
from APPLY_OUT where "CLAIM_ID" = 'CL_1027985'
)
order by 1, 2;
