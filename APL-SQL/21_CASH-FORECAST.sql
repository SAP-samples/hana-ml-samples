drop table FUNC_HEADER;
create table FUNC_HEADER like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.FUNCTION_HEADER";
insert into FUNC_HEADER values ('Oid', '#42');
insert into FUNC_HEADER values ('LogLevel', '8');

-- Data sorted over time
drop view TS_SORTED;
create view TS_SORTED as select * from APL_SAMPLES.CASHFLOWS_FULL order by "Date" asc;

drop table FORECAST_CONFIG;
create table FORECAST_CONFIG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_CONFIG_EXTENDED";
insert into FORECAST_CONFIG values ('APL/Horizon', '21',null);
insert into FORECAST_CONFIG values ('APL/TimePointColumnName', 'Date',null);
insert into FORECAST_CONFIG values ('APL/LastTrainingTimePoint', '2001-12-29 00:00:00',null);
insert into FORECAST_CONFIG values ('APL/ApplyExtraMode','Forecasts and Error Bars',null);

drop table VARIABLE_DESC;
create table VARIABLE_DESC like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_DESC_OID";
insert into VARIABLE_DESC  select *, '#42' from APL_SAMPLES.CASHFLOWS_DESC;

drop table VARIABLE_ROLES;
create table VARIABLE_ROLES like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.VARIABLE_ROLES_WITH_COMPOSITES_OID";
insert into VARIABLE_ROLES values ('Date', 'input',NULL,NULL,'#42');
insert into VARIABLE_ROLES values ('Cash', 'target',NULL,NULL,'#42');

-- Structure of the output
drop type FORECAST_OUT_T;
create type FORECAST_OUT_T as table (
	"Date" 	DATE,
	"Cash" DOUBLE,
	"kts_1" DOUBLE,
    "kts_1_lowerlimit_95%" DOUBLE,
    "kts_1_upperlimit_95%" DOUBLE
);

drop table FORECAST_OUT;
create table FORECAST_OUT like FORECAST_OUT_T;

drop table OPERATION_LOG;
create table OPERATION_LOG like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.OPERATION_LOG";

drop table SUMMARY;
create table SUMMARY like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.SUMMARY";

drop table INDICATORS;
create table INDICATORS like "SAP_PA_APL"."sap.pa.apl.base::BASE.T.INDICATORS";

call "SAP_PA_APL"."sap.pa.apl.base::FORECAST" (
FUNC_HEADER, FORECAST_CONFIG, VARIABLE_DESC, VARIABLE_ROLES, 
'USER_APL', 'TS_SORTED',   -- Business data
'USER_APL', 'FORECAST_OUT', OPERATION_LOG, SUMMARY, INDICATORS
) with overview;


--- Actual Vs Forecast
SELECT 
 F."Date", 
 round(A."Cash",1) as "Actual", 
 round(F."kts_1",1) as "Estimate",
 round(F."kts_1_lowerlimit_95%",1) as "Lower Limit",
 round(F."kts_1_upperlimit_95%",1) as "Upper Limit"
FROM FORECAST_OUT F, TS_SORTED A
WHERE A."Date" = F."Date"
ORDER BY F."Date"
;