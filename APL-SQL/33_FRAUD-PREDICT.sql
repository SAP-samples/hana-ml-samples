drop table FUNC_HEADER;
create table FUNC_HEADER like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
insert into FUNC_HEADER values ('Oid', 'Claims');
insert into FUNC_HEADER values ('LogLevel', '8');

drop table APPLY_CONFIG;
create table APPLY_CONFIG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_EXTENDED";
insert into APPLY_CONFIG values ('APL/ApplyExtraMode', 'Advanced Apply Settings',null);
insert into APPLY_CONFIG values ('APL/ApplyPredictedValue', 'true',null);
insert into APPLY_CONFIG values ('APL/ApplyProbability', 'true',null);
insert into APPLY_CONFIG values ('APL/ApplyDecision', 'true',null);
insert into APPLY_CONFIG values ('APL/ApplyReasonCode/TopCount', '3',null);
--insert into APPLY_CONFIG values ('APL/ApplyReasonCode/BottomCount', '3',null);
insert into APPLY_CONFIG values ('APL/ApplyReasonCode/ShowStrengthValue', 'false',null);
insert into APPLY_CONFIG values ('APL/ApplyReasonCode/ShowStrengthIndicator', 'false',null);
insert into APPLY_CONFIG values ('APL/ApplyReasonCode/ShowOtherStrength', 'false',null);
insert into APPLY_CONFIG values ('APL/ApplyReasonCode/RankOnAbsoluteValues', 'false',null);

drop table SCHEMA_OUT;
create COLUMN table SCHEMA_OUT like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.TABLE_TYPE";

drop table SCHEMA_LOG;
create COLUMN table SCHEMA_LOG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";

call "SAP_PA_APL"."sap.pa.apl.base::GET_TABLE_TYPE_FOR_APPLY"(
 FUNC_HEADER, MODEL_TRAIN_BIN, APPLY_CONFIG, 
 'APL_SAMPLES', 'AUTO_CLAIMS_NEW',      
 SCHEMA_OUT , SCHEMA_LOG
) with overview;

drop table APPLY_OUT;
call "SAP_PA_APL"."sap.pa.apl.base::CREATE_TABLE_FROM_TABLE_TYPE" (
 'USER_APL','APPLY_OUT', SCHEMA_OUT 
);

drop table APPLY_LOG;
create column table APPLY_LOG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";

drop table SUMMARY;
create column table SUMMARY like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";

call "SAP_PA_APL"."sap.pa.apl.base::APPLY_MODEL" (
 FUNC_HEADER, MODEL_TRAIN_BIN, APPLY_CONFIG, 
 'APL_SAMPLES', 'AUTO_CLAIMS_NEW',      
 'USER_APL', 'APPLY_OUT',              
 APPLY_LOG, SUMMARY
) with overview;

select 
 "CLAIM_ID" as "Claim Id", 
 round("gb_score_IS_FRAUD",2) as "Score", 
 "gb_decision_IS_FRAUD" as "Decision",
 round("gb_proba_IS_FRAUD",2) as "Probability",
 "gb_reason_top_1_name" as "Top 1 Name", "gb_reason_top_1_value" as "Top 1 Value",
 "gb_reason_top_2_name" as "Top 2 Name", "gb_reason_top_2_value" as "Top 2 Value",
 "gb_reason_top_3_name" as "Top 3 Name", "gb_reason_top_3_value" as "Top 3 Value"
from APPLY_OUT 
order by 2 desc;
