drop view SORTED_4TRAIN;
create view SORTED_4TRAIN as 
select * from apl_samples.census 
where "marital-status" not in (
  select "marital-status" from apl_samples.census 
  group by "marital-status" having count(*) < 1500 )
order by "id"
;

drop table MODEL_BIN;
create table MODEL_BIN like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.MODEL_BIN_OID";

drop table OP_LOG;
create table OP_LOG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";

drop table DEBRIEF_METRIC;
create table DEBRIEF_METRIC like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_METRIC_OID";

drop table DEBRIEF_PROPERTY;
create table DEBRIEF_PROPERTY like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_PROPERTY_OID";

DO BEGIN
    declare header "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
    declare config "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_DETAILED";   
    declare var_desc "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_DESC_OID";      
    declare var_role "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_ROLES_WITH_COMPOSITES_OID";      
    declare apl_model "SAP_PA_APL"."sap.pa.apl.base::BASE.T.MODEL_BIN_OID";      
    declare apl_log   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";      
    declare apl_sum   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";      
    declare apl_metr "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_METRIC_OID";
    declare apl_prop "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_PROPERTY_OID";    

    :header.insert(('Oid', 'My Multiclass Model'));

    :config.insert(('APL/ModelType', 'multiclass',null));
    :config.insert(('APL/CuttingStrategy', 'random with no test',null));
    :config.insert(('APL/VariableAutoSelection', 'true',null));

    :var_desc.insert((0,'id','number','nominal',1,0,null,null,null,null));
    :var_desc.insert((1,'age','number','continuous',0,0,null,null,null,null));
    :var_desc.insert((2,'workclass','string','nominal',0,0,'?',null,null,null));
    :var_desc.insert((3,'fnlwgt','number','continuous',0,0,null,null,null,null));	
    :var_desc.insert((4,'education','string','nominal',0,0,null,null,null,null));
    :var_desc.insert((5,'education-num','number','ordinal',0,0,null,null,null,null));
    :var_desc.insert((6,'marital-status','string','nominal',0,0,null,null,null,null));	
    :var_desc.insert((7,'occupation','string','nominal',0,0,null,null,null,null));
    :var_desc.insert((8,'relationship','string','nominal',0,0,null,null,null,null));		
    :var_desc.insert((9,'race','string','nominal',0,0,null,null,null,null));	
    :var_desc.insert((10,'sex','string','nominal',0,0,null,null,null,null));	
    :var_desc.insert((11,'capital-gain','number','continuous',0,0,'99999',null,null,null));	
    :var_desc.insert((12,'capital-loss','number','continuous',0,0,null,null,null,null));
    :var_desc.insert((13,'hours-per-week','number','continuous',0,0,null,null,null,null));	
    :var_desc.insert((14,'native-country','string','nominal',0,0,'?',null,null,null));
    :var_desc.insert((15,'class','number','nominal',0,0,null,null,null,null));	

    :var_role.insert(('id', 'skip',null,null,null));
    :var_role.insert(('marital-status', 'target',null,null,null));

    "SAP_PA_APL"."sap.pa.apl.base::CREATE_MODEL_AND_TRAIN_DEBRIEF"(
	:header, :config, :var_desc, :var_role, 
	'USER_APL','SORTED_4TRAIN', 
	apl_model, apl_log, apl_sum, apl_metr, apl_prop);
 
    insert into  MODEL_BIN  select * from :apl_model;
	insert into  OP_LOG     select * from :apl_log;
	insert into  SUMMARY    select * from :apl_sum;
	insert into  DEBRIEF_METRIC    select * from :apl_metr;
	insert into  DEBRIEF_PROPERTY  select * from :apl_prop;
END;

select * from SUMMARY;

select * from
"SAP_PA_APL"."sap.pa.apl.debrief.report::ClassificationRegression_VariablesContribution"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC)
where "Contribution" > 0
order by 1, 3;

select * from
"SAP_PA_APL"."sap.pa.apl.debrief.report::ClassificationRegression_Performance"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC)
where "Partition" = 'Validation';

select * from
"SAP_PA_APL"."sap.pa.apl.debrief.report::ClassificationRegression_VariablesExclusion"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC);

select * from
"SAP_PA_APL"."sap.pa.apl.debrief.report::Classification_MultiClass_ConfusionMatrix"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC)
where "Partition" = 'Validation';
