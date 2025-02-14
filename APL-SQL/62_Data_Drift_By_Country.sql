--- Comparing 2 populations by Country
create local temporary column table "#DATASET_1" as (
 select "age", "occupation", "workclass", "education", "relationship", "native-country" 
 from APL_SAMPLES.CENSUS 
 where "native-country" in ('United-States','Mexico') and "sex" = 'Male' 
 order by "native-country", "id" 
);

create local temporary column table "#DATASET_2" as (
 select "age", "occupation", "workclass", "education", "relationship", "native-country" 
 from APL_SAMPLES.CENSUS 
 where "native-country" in ('United-States','Mexico') and "sex" = 'Female' 
 order by "native-country", "id" 
);

DO BEGIN
    declare header "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
    declare config "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_EXTENDED";   
    declare var_desc "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_DESC_OID";      
    declare var_role "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_ROLES_WITH_COMPOSITES_OID";      
    declare out_log   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";             
    declare out_summary  "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";   
    declare out_metric   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_METRIC_OID";      
    declare out_property "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_PROPERTY_OID";
 
    :header.insert(('Oid', 'Male Vs Female'));

    :config.insert(('APL/SegmentColumnName', 'native-country',null)); 
	
	"SAP_PA_APL"."sap.pa.apl.base::COMPARE_DATA" (
	:header, :config, :var_desc, :var_role,'USER_APL','#DATASET_1', 'USER_APL','#DATASET_2', 
	out_log, out_summary, out_metric, out_property );
 
    select * from SAP_PA_APL."sap.pa.apl.debrief.report::Deviation_ByVariable"(:out_property,:out_metric);
    select * from SAP_PA_APL."sap.pa.apl.debrief.report::Deviation_ByCategory"(:out_property,:out_metric, Deviation_Threshold => 0.9); 
    select * from SAP_PA_APL."sap.pa.apl.debrief.report::Deviation_CategoryFrequencies"(:out_property,:out_metric); 
END;
