drop table FUNC_HEADER;
create table FUNC_HEADER like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
insert into FUNC_HEADER values ('Oid', 'Claims');
insert into FUNC_HEADER values ('LogLevel', '8');
insert into FUNC_HEADER values ('ModelFormat', 'bin');

drop table CREATE_AND_TRAIN_CONFIG;
create table CREATE_AND_TRAIN_CONFIG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_EXTENDED";
insert into CREATE_AND_TRAIN_CONFIG values ('APL/ModelType', 'binary classification',null);
insert into CREATE_AND_TRAIN_CONFIG values ('APL/CuttingStrategy', 'random with no test',null);

drop table VARIABLE_DESC;
create table VARIABLE_DESC like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_DESC_OID";
insert into VARIABLE_DESC values (0,'CLAIM_ID','string','nominal',1,0,NULL,NULL,'Unique Identifier of a claim',NULL);
insert into VARIABLE_DESC values (1,'DAYS_TO_REPORT','integer','continuous',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (2,'BODILY_INJURY_AMOUNT','integer','continuous',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (3,'PROPERTY_DAMAGE','integer','continuous',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (4,'PREVIOUS_CLAIMS','integer','ordinal',0,0,NULL,NULL,'Number of previous claims',NULL);
insert into VARIABLE_DESC values (5,'PAYMENT_METHOD','string','nominal',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (6,'IS_REAR_END_COLLISION','string','nominal',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (7,'PREM_AMOUNT','string','nominal',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (8,'AGE','integer','continuous',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (9,'GENDER','string','nominal',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (10,'MARITAL_STATUS','string','nominal',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (11,'INCOME_ESTIMATE','number','continuous',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (12,'INCOME_CATEGORY','integer','ordinal',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (13,'POLICY_HOLDER','string','nominal',0,0,NULL,NULL,NULL,NULL);
insert into VARIABLE_DESC values (14,'IS_FRAUD','string','nominal',0,0,NULL,NULL,'Yes/No flag',NULL);

drop table VARIABLE_ROLES;
create table VARIABLE_ROLES like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_ROLES_WITH_COMPOSITES_OID";
insert into VARIABLE_ROLES values ('IS_FRAUD', 'target',null,null,null);
insert into VARIABLE_ROLES values ('CLAIM_ID', 'skip',null,null,null);

drop table MODEL_TRAIN_BIN;
create table MODEL_TRAIN_BIN like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.MODEL_BIN_OID";

drop table OPERATION_LOG;
create table OPERATION_LOG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";

drop table SUMMARY;
create table SUMMARY like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";

drop table INDICATORS;
create table INDICATORS like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.INDICATORS";

-- Run the APL function 
call "SAP_PA_APL"."sap.pa.apl.base::CREATE_MODEL_AND_TRAIN"(
 FUNC_HEADER, CREATE_AND_TRAIN_CONFIG, VARIABLE_DESC, VARIABLE_ROLES, 	-- APL Inputs
 'APL_SAMPLES', 'AUTO_CLAIMS_FRAUD', 									-- Business data
 MODEL_TRAIN_BIN, OPERATION_LOG, SUMMARY, INDICATORS					-- APL Outputs
) with overview;

select key as "Item", value as "Value" from summary;
