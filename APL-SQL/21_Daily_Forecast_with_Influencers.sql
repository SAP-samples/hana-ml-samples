-- Input Series sorted over time with candidate predictors 
drop view TS_SORTED;
create view TS_SORTED as select "Date", "Cash", "MondayMonthInd", "FridayMonthInd" 
from APL_SAMPLES.CASHFLOWS_FULL order by 1 asc;

--- Output Tables
drop table FORECAST_OUT;
create table FORECAST_OUT  (
	"Date" 	DATE,
	"Cash" DOUBLE,
	"kts_1" DOUBLE,
	"kts_1Trend" DOUBLE,
	"kts_1Cycles" DOUBLE,
    "kts_1_lowerlimit_95%" DOUBLE,
    "kts_1_upperlimit_95%" DOUBLE,
	"kts_1ExtraPreds" DOUBLE,
	"kts_1Fluctuations" DOUBLE,
	"kts_1Residues" DOUBLE
);

drop table OP_LOG;
create table OP_LOG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";

drop table SUMMARY;
create table SUMMARY like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";

drop table INDICATORS;
create table INDICATORS like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.INDICATORS";

drop table DEBRIEF_METRIC;
create table DEBRIEF_METRIC like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_METRIC_OID";

drop table DEBRIEF_PROPERTY;
create table DEBRIEF_PROPERTY like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_PROPERTY_OID";

DO BEGIN
    declare header "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
    declare config "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_DETAILED";
	declare var_desc "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_DESC_OID";   
    declare var_role "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_ROLES_WITH_COMPOSITES_OID";    
    declare apl_log   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";      
    declare apl_sum   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";   
    declare apl_indic   "SAP_PA_APL"."sap.pa.apl.base::BASE.T.INDICATORS";   	
    declare apl_metr "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_METRIC_OID";
    declare apl_prop "SAP_PA_APL"."sap.pa.apl.base::BASE.T.DEBRIEF_PROPERTY_OID";      

    :header.insert(('Oid', 'Daily Cash Flow'));

    :config.insert(('APL/Horizon', '21',null));
    :config.insert(('APL/TimePointColumnName', 'Date',null));
    :config.insert(('APL/LastTrainingTimePoint', '2001-12-29 00:00:00',null));
	:config.insert(('APL/DecomposeInfluencers', 'true',null));
	:config.insert(('APL/ApplyExtraMode', 'First Forecast with Stable Components and Residues and Error Bars',null));
	
    :var_role.insert(('Date', 'input', null, null, null));
    :var_role.insert(('Cash', 'target', null, null, null));
	
	:var_desc.insert((0,'Date','datetime','continuous',1,1,null,null,'Unique Id',null));
    :var_desc.insert((1,'Cash','number','continuous',0,0,null,null,null,null));
    :var_desc.insert((2,'MondayMonthInd','integer','ordinal',0,0,null,null,null,null));
    :var_desc.insert((3,'FridayMonthInd','integer','ordinal',0,0,null,null,null,null));
	
    "SAP_PA_APL"."sap.pa.apl.base::FORECAST_AND_DEBRIEF"(
	:header, :config, :var_desc, :var_role, 
	'USER_APL','TS_SORTED', 
	'USER_APL', 'FORECAST_OUT', apl_log, apl_sum, apl_indic, apl_metr, apl_prop);

	insert into  OP_LOG     select * from :apl_log;
	insert into  SUMMARY    select * from :apl_sum;
	insert into  INDICATORS    select * from :apl_indic;
	insert into  DEBRIEF_METRIC    select * from :apl_metr;
	insert into  DEBRIEF_PROPERTY  select * from :apl_prop;
END;

drop view DECOMPOSED_SERIES;
create view DECOMPOSED_SERIES 
("Time","Actual","Forecast","Trend","Cycles","Lower_Limit","Upper_Limit",
 "Influencers","Fluctuations","Residuals") as
SELECT * FROM FORECAST_OUT ORDER BY 1;

SELECT "Time",
 round("Actual",2) as "Actual",
 round("Forecast",2) as "Forecast",
 round("Trend",2) as "Trend",
 round("Cycles",2) as "Cycles",
 round("Influencers",2) as "Influencers",
 round("Fluctuations",2) as "AR",
 round("Residuals",2) as "Residuals"
FROM DECOMPOSED_SERIES ORDER BY 1;
 
 select * from 
"SAP_PA_APL"."sap.pa.apl.debrief.report::TimeSeries_ModelOverview"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC);

select * from
"SAP_PA_APL"."sap.pa.apl.debrief.report::TimeSeries_Performance"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC)
where 
"Partition" = 'Validation';

select * from
"SAP_PA_APL"."sap.pa.apl.debrief.report::TimeSeries_Components"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC);

select * from
"SAP_PA_APL"."sap.pa.apl.debrief.report::TimeSeries_Decomposition"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC)
order by 1, 2;

select * from
"SAP_PA_APL"."sap.pa.apl.debrief.report::TimeSeries_DetrendedExtraPredictable"
(USER_APL.DEBRIEF_PROPERTY, USER_APL.DEBRIEF_METRIC)
where 
"Partition" = 'Estimation';
