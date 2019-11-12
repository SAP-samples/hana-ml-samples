-- Features Importance 
select 
 OID as "Model Id", DETAIL as "Method", 
 row_number() OVER (partition by OID order by to_double(TO_NVARCHAR(VALUE)) desc) as "Rank",
 VARIABLE as "Explanatory Variable", 
 round(to_double(TO_NVARCHAR(VALUE)) *100 , 2) as "Contribution",
 round(sum(to_double(TO_NVARCHAR(VALUE))) OVER (partition by OID order by to_double(TO_NVARCHAR(VALUE)) desc) *100 ,2) 
 as "Cumulative"
from 
 INDICATORS 
where 
 TARGET = 'IS_FRAUD' and  KEY = 'VariableContribution'
order by 3;

--- Model Overview 
select 
 ltrim(key,'Model') as "Item", value as "Value"
from 
 SUMMARY
where 
 key in ('ModelVariableCount','ModelSelectedVariableCount','ModelRecordCount','ModelBuildDate')
;

-- Accuracy  
select 
 OID as "Model Id", TARGET as "Target", round(to_double(VALUE), 4) as "AUC"  
from 
 INDICATORS  
where VARIABLE like 'gb_score%' and KEY = 'AUC'  
order by 1;

-- Target Frequency
select 
 "VARIABLE" as "Target Variable", DETAIL as "Value",
 round(to_double(VALUE) *100, 2) as "Frequency"
from 
 INDICATORS 
where variable = 'IS_FRAUD' and key = 'CategoryFrequency';

-- Descriptive Stats
With TB as (
select 
 "VARIABLE" , KEY, round(to_double(VALUE), 4) as "VALUE" 
from 
 INDICATORS 
where KEY in ('Min', 'Max', 'Mean', 'StandardDeviation')
) 
select "VARIABLE" as "Numeric Variable",
 max(Case KEY When 'Min' Then "VALUE" Else null End) as "Minimum",
 max(Case KEY When 'Max' Then "VALUE" Else null End) as "Maximum",
 round(max(Case KEY When 'Mean' Then "VALUE" Else null End),2) as "Mean",
 round(max(Case KEY When 'StandardDeviation' Then "VALUE" Else null End),2) as "Standard Deviation"
from 
 TB  
group by "VARIABLE"
order by 1;

--- Partitions
With TB as (
 select to_double(value) as TOTAL 
 from SUMMARY 
 where key = 'ModelRecordCount'
) 
select 
 ltrim(substr_after(key, 'ModelRecord'),'Count') as "Partition", 
 to_double(value) as "Number of rows", 
 round(to_double(value) / TB.TOTAL *100, 2) as "In %" 
from 
 SUMMARY, TB
where 
  KEY like 'ModelRecord%' and value <> '0' and key <> 'ModelRecordCount'
order by 1
;