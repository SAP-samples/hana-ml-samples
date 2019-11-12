--- Historical data
SELECT 
 to_char("Date",'YYYY') as "Year",
 min(Case When month("Date") = 1 Then "OzoneRateLA" Else null End) as "Jan",
 min(Case When month("Date") = 2 Then "OzoneRateLA" Else null End) as "Feb",
 min(Case When month("Date") = 3 Then "OzoneRateLA" Else null End) as "Mar",
 min(Case When month("Date") = 4 Then "OzoneRateLA" Else null End) as "Apr",
 min(Case When month("Date") = 5 Then "OzoneRateLA" Else null End) as "May",
 min(Case When month("Date") = 6 Then "OzoneRateLA" Else null End) as "Jun",
 min(Case When month("Date") = 7 Then "OzoneRateLA" Else null End) as "Jul",
 min(Case When month("Date") = 8 Then "OzoneRateLA" Else null End) as "Aug",
 min(Case When month("Date") = 9 Then "OzoneRateLA" Else null End) as "Sep",
 min(Case When month("Date") =10 Then "OzoneRateLA" Else null End) as "Oct",
 min(Case When month("Date") =11 Then "OzoneRateLA" Else null End) as "Nov",
 min(Case When month("Date") =12 Then "OzoneRateLA" Else null End) as "Dec"
FROM TS_SORTED 
group by to_char("Date",'YYYY')
ORDER BY 1
;

--- Components
SELECT
 KEY as "Component Name", 
 CASE WHEN to_char(VALUE) ='' THEN '<none>' ELSE VALUE END as "Component Value"
FROM INDICATORS 
WHERE KEY in ('Trend','Cycles','Fluctuations')
;

--- Overview 
select 
 ltrim(ltrim(key,'Model'),'Time') as key, value
from 
 SUMMARY
where 
 key in ('ModelVariableCount','ModelSelectedVariableCount','ModelRecordCount','ModelBuildDate') or
 key in ('ModelTimeSeriesFirstDate','ModelTimeSeriesLastDate','ModelTimeSeriesHorizon')
;
---- Descriptive Stats
SELECT 
 VARIABLE as "Target", 
 KEY as "Statistic", 
 round(to_double(VALUE),4) as "Value"
FROM 
 INDICATORS
WHERE 
 VARIABLE = 'OzoneRateLA' AND 
 KEY <> 'CategoryFrequency'
;
---- Outliers
SELECT 
A."Date", A."OzoneRateLA" as "Outlier"
FROM 
OPERATION_LOG L, TS_SORTED A
WHERE 
MESSAGE like '%outlier has been detected at time point (%' AND
to_date(substr(substr_after(L.MESSAGE,'at time point ('),1,10),'YYYY-MM-DD') = A."Date"
;

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

 
---- ACCURACY 
select 
 round(avg(Case KEY When 'L1' Then to_number(value) Else Null End),3) as "MAE",
 round(avg(Case KEY When 'MAPE' Then to_number(value) Else Null End) *100,2) as "MAPE",
 round(avg(Case KEY When 'SMAPE' Then to_number(value) Else Null End) *100,2) as "SMAPE",
 round(avg(Case KEY When 'L2' Then to_number(value) Else Null End),3) as "RMSE",
 round(avg(Case KEY When 'R2' Then to_number(value) Else Null End),4) as "R2"
from  
 INDICATORS 
where 
  KEY in ('R2', 'L2', 'SMAPE', 'MAPE','L1');
