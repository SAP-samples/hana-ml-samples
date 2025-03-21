--- Input
create local temporary column table "#DATASET_1" as (
 select "age", "occupation", "workclass", "education", "relationship" 
 from APL_SAMPLES.CENSUS 
 where "sex" = 'Male' order by "id" 
);

--- Output 
drop table APL_MODEL;
create table APL_MODEL like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.MODEL_BIN_OID";

DO BEGIN
    declare header "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
    declare config "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_DETAILED";   
    declare var_desc "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_DESC_OID";      
    declare var_role "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_ROLES_WITH_COMPOSITES_OID";      
    declare out_model "SAP_PA_APL"."sap.pa.apl.base::BASE.T.MODEL_BIN_OID";      
    declare out_log   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";             
    declare out_summary  "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";   
    declare out_metric   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_METRIC_OID";      
    declare out_property "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_PROPERTY_OID";     

    :header.insert(('Oid', 'DataDrift'));

    :config.insert(('APL/ModelType', 'statbuilder',null));

    "SAP_PA_APL"."sap.pa.apl.base::CREATE_MODEL_AND_TRAIN_DEBRIEF" (
	:header, :config, :var_desc, :var_role, 'USER_APL','#DATASET_1', 
	out_model, out_log, out_summary, out_metric, out_property );

    insert into APL_MODEL select * from :out_model;
	
    select * from SAP_PA_APL."sap.pa.apl.debrief.report::Statistics_CategoryFrequencies"(:out_property,:out_metric);
END;
