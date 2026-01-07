/*****************************************************************************************************************************************************************************/
/*** Sample prepared 07.01.2026                                                                                                                                            ***/   
/*****************************************************************************************************************************************************************************/
/*** Using SAP HANA Cloud Predictive Analysis Library(PAL) ML model state for real-time prediction performance.
 *   During regular ML model predictions with PAL models, in a first step the model is accessed from a database table source and parsed into a binary, runtime model object 
 *   before it can be applied with the prediction- or model scoring-functions.
 *   For every single prediction- or model scoring-function call, the overhead of reading the model data and parsing it into a binary, runtime model object will be included.
 *   Hence for ML scenarios, where 
 * 		- either the trained model is very complex then also the parsing of the model may take too much time compared with the expected total execution time of predict functions
 *      - or in general, the prediction runtime shall be as minimal as possible and near real-time
 *   avoiding to parse the model over and over again is definitely beneficial.
 *   To achive this, splitting the model parsing from the actual prediction exectuion can be achieved by a family of PAL functions called stated enabled functions. 
 *   As the name suggests, after being parsed, the model is kept in a runtime memory object called state. 
 *   There are four types of operation task regarding the use and management of PAL ML model state
 *      - creating a PAL ML model state
 * 		- listing PAL ML models with state
 * 		- apply the PAL ML models with state using a predict- or score-funtion
 * 		- delete a PAL ML model state.
 *   For a detailed description see the documentation at 
 * 		- https://help.sap.com/docs/hana-cloud-database/sap-hana-cloud-sap-hana-database-predictive-analysis-library/state-enabled-real-time-scoring-functions
 ***/
/******************************************************************************************************************************************************************************/

/******************************************************************************************************************************************************************************/
/*** Step 0 - Preparation *****************************************************************************************************************************************************/
SET SCHEMA DM_PAL;

/*---Prepare data for a Unified Classification(Random decision trees) training process -----------------------------*/
DROP TABLE TRAIN_DATA;
create column table TRAIN_DATA ("ATT1" double, "ATT2" double, "ATT3" double, "ATT4" double, "LABEL" nvarchar(50));
insert into TRAIN_DATA values (1.0, 10.0, 100, 1.0, 'A');
insert into TRAIN_DATA values (1.1, 10.1, 100, 1.0, 'A');
insert into TRAIN_DATA values (1.2, 10.2, 100, 1.0, 'A');
insert into TRAIN_DATA values (1.3, 10.4, 100, 1.0, 'A');
insert into TRAIN_DATA values (1.2, 10.3, 100, 1.0, 'A');
insert into TRAIN_DATA values (4.0, 40.0, 400, 4.0, 'B');
insert into TRAIN_DATA values (4.1, 40.1, 400, 4.0, 'B');
insert into TRAIN_DATA values (4.2, 40.2, 400, 4.0, 'B');
insert into TRAIN_DATA values (4.3, 40.4, 400, 4.0, 'B');
insert into TRAIN_DATA values (4.2, 40.3, 400, 4.0, 'A');
insert into TRAIN_DATA values (9.0, 90.0, 900, 2.0, 'A');
insert into TRAIN_DATA values (9.1, 90.1, 900, 1.0, 'B');
insert into TRAIN_DATA values (9.2, 90.2, 900, 2.0, 'B');
insert into TRAIN_DATA values (9.3, 90.4, 900, 1.0, 'B');
insert into TRAIN_DATA values (9.2, 90.3, 900, 1.0, 'B');

drop table PREDICT_DATA;
create column table PREDICT_DATA ("ID" integer,"ATT1" double, "ATT2" double, "ATT3" double, "ATT4" double);
insert into PREDICT_DATA values (1, 1.0, 10.0, 100, 1);
insert into PREDICT_DATA values (2, 1.1, 10.1, 100, 1);
insert into PREDICT_DATA values (3, 1.2, 10.2, 100, 1);
insert into PREDICT_DATA values (4, 1.3, 10.4, 100, 1);
insert into PREDICT_DATA values (5, 1.2, 10.3, 100, 3);
insert into PREDICT_DATA values (6, 4.0, 40.0, 400, 3);
insert into PREDICT_DATA values (7, 4.1, 40.1, 400, 3);
insert into PREDICT_DATA values (8, 4.2, 40.2, 400, 3);
insert into PREDICT_DATA values (9, 4.3, 40.4, 400, 3);
insert into PREDICT_DATA values (10,4.2, 40.3, 400, 3);

DROP TABLE PAL_MODEL_RDT;  
CREATE COLUMN TABLE PAL_MODEL_RDT ("ROW_INDEX" INTEGER,"TREE_INDEX" INTEGER,"MODEL_CONTENT" NVARCHAR(5000));
Truncate table  PAL_MODEL_RDT;

/*--- Train the Classification(Random decision trees) ML model --------------------------------------------------------*/
DO
BEGIN
  DECLARE lt_PAL_PARAMETER_TBL TABLE("PARAM_NAME" NVARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100));
  lt_data = SELECT * FROM TRAIN_DATA;
  :lt_PAL_PARAMETER_TBL.INSERT(('FUNCTION', NULL, NULL, 'RDT'),	1);
  :lt_PAL_PARAMETER_TBL.INSERT(('N_ESTIMATORS', 200, NULL, NULL),		2);
  :lt_PAL_PARAMETER_TBL.INSERT(('SEED', 1234, NULL, NULL),			3);
  :lt_PAL_PARAMETER_TBL.INSERT(('SPLIT_THRESHOLD', NULL, 0.0000001,  NULL),4);
  :lt_PAL_PARAMETER_TBL.INSERT(('THREAD_RATIO', NULL, 0.8, NULL),7);
  :lt_PAL_PARAMETER_TBL.INSERT(('TREES_NUM',200, NULL, NULL),8);
  :lt_PAL_PARAMETER_TBL.INSERT(('LABEL',NULL,NULL,'LABEL'),9);
  :lt_PAL_PARAMETER_TBL.INSERT(('ALLOW_MISSING_LABEL',1, NULL,NULL),9); 
  :lt_PAL_PARAMETER_TBL.INSERT(('MAX_DEPTH', 55,    NULL, NULL),10); 
  :lt_PAL_PARAMETER_TBL.INSERT(('SAMPLE_FRACTION', NULL, 1.0 , NULL),12);
  :lt_PAL_PARAMETER_TBL.INSERT(('PARTITION_METHOD',2, NULL, NULL),17);
  :lt_PAL_PARAMETER_TBL.INSERT(('PARTITION_STRATIFIED_VARIABLE', NULL, NULL, 'LABEL'),19);
  :lt_PAL_PARAMETER_TBL.INSERT(('PARTITION_TRAINING_PERCENT', NULL, 0.7, NULL),20);
  :lt_PAL_PARAMETER_TBL.INSERT(('PARTITION_RANDOM_SEED', 1234, NULL, NULL),21);
  :lt_PAL_PARAMETER_TBL.INSERT(('DECILE', 20, NULL, NULL),22); 
  :lt_PAL_PARAMETER_TBL.INSERT(('KEY', 0, NULL, NULL),23); 
  :lt_PAL_PARAMETER_TBL.INSERT(('CATEGORICAL_VARIABLE', NULL, NULL, 'LABEL'),24);
	CALL _SYS_AFL.PAL_UNIFIED_CLASSIFICATION (:lt_data, :lt_PAL_PARAMETER_TBL, lt_model, lt_imp, lt_stat, lt_opt, lt_cm, lt_metrics, lt_ph1, lt_ph2);																							
	INSERT INTO PAL_MODEL_RDT SELECT * FROM :lt_model;	
END;

SELECT * FROM PAL_MODEL_RDT;


/******************************************************************************************************************************************************************************/
/*** Step 1 - Create MODEL State and check model ******************************************************************************************************************************/
SELECT * FROM PREDICT_DATA;

/*--- Note, with Unified Classification ML model can be created implicitly using the predict-function and the STATE_ID parameter ---*/
DO
BEGIN
  DECLARE lt_PAL_PARAMETER_TBL TABLE("PARAM_NAME" NVARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100));
  :lt_PAL_PARAMETER_TBL.INSERT(('FUNCTION', NULL, NULL, 'RDT'),	1);
  :lt_PAL_PARAMETER_TBL.INSERT(('HAS_ID', 1, NULL, NULL),		2);
  :lt_PAL_PARAMETER_TBL.INSERT(('VERBOSE', 0, NULL, NULL),			3);
  :lt_PAL_PARAMETER_TBL.INSERT(('FEATURE_ATTRIBUTION_METHOD', 0,  NULL,  NULL),4);
  :lt_PAL_PARAMETER_TBL.INSERT(('STATE_ID', NULL, NULL, 'RDT MODEL 2025.Q4.v1'),5); -- you can specify a custom state-ID using the STATE_ID parameter with the UC-predict function;
  :lt_PAL_PARAMETER_TBL.INSERT(('THREAD_RATIO', NULL, 0.8, NULL),6);
  
  	lt_data = SELECT * FROM PREDICT_DATA ;
	lt_mdl = SELECT * FROM PAL_MODEL_RDT;
	CALL _SYS_AFL.PAL_UNIFIED_CLASSIFICATION_PREDICT (:lt_data, :lt_mdl, :lt_PAL_PARAMETER_TBL, lt_predresult, lt_empty);
	SELECT TOP 10 * FROM :lt_predresult;	
END;
/*
ID|SCORE|CONFIDENCE|REASON_CODE|
--+-----+----------+-----------+
 1|A    |       1.0|           |
 2|A    |       1.0|           |
 3|A    |       1.0|           |
 4|A    |       1.0|           |
 5|A    |      0.93|           | 
 */

SELECT * FROM PUBLIC.M_AFL_STATES; -- must be dbadmin to review M_AFL_STATES;
SELECT * FROM SYS.M_AFL_STATES; -- must be dbadmin to review M_AFL_STATES;
/*
|STATE_ID            |AREA_NAME|DESCRIPTION           |CREATE_TIMESTAMP       |LAST_ACCESS_TIMESTAMP  |HOST                                |PORT  |
|--------------------|---------|----------------------|-----------------------|-----------------------|------------------------------------|------|
|RDT MODEL 2025.Q4.v1|AFLPAL   |State of AFL_PAL Model|2026-01-07 13:24:55.474|2026-01-07 13:24:55.480|34e77cb1-06e0-4ef3-9ab9-42acaa66033e|30,046|
*/


/**********************************************************************************************************************************************************************************/
/*** Step 2 - Predict applying ML models with state and check execution runtimes of real-time(stateful) model predictions *********************************************************/

DROP TABLE EMPTY_MODEL_TBL; -- an empty model table needs to be provided with the prediction call;
create column table EMPTY_RDTMODEL like PAL_MODEL_RDT;

DROP TABLE RUNTIME_AUDIT; -- we want to store execution runtimes of real-time(stateful) model predictions into a table;
Create column table RUNTIME_AUDIT ( "TEST_NAME" VARCHAR(100), "RUN_ID" VARCHAR(8), "BEGIN_TIME" TIMESTAMP, "END_TIME" TIMESTAMP, ELAPSED_TIME  VARCHAR(36));
truncate TABLE RUNTIME_AUDIT;

/*--- Prepare a simple procedure for model prediction using PAL ML models with state ---*/
CREATE OR REPLACE PROCEDURE PAL_FAST_PREDICT (IN PREDICTID INTEGER ) 
LANGUAGE SQLSCRIPT
    SQL SECURITY INVOKER
    AS
 /* For measuring the runtime of individual call, we use SEQUENTIAL EXECUTION  */
 BEGIN SEQUENTIAL EXECUTION 
  declare t1 timestamp;
  declare t2 timestamp;
  declare t4 timestamp;
  declare t5 timestamp;
  declare ETIME VARCHAR(36);
  declare ETIME5 VARCHAR(36);
  DECLARE RUNID INT;
  DECLARE lt_PAL_PARAMETER_TBL TABLE("PARAM_NAME" NVARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100));
  :lt_PAL_PARAMETER_TBL.INSERT(('FUNCTION', NULL, NULL, 'RDT'));
  :lt_PAL_PARAMETER_TBL.INSERT(('HAS_ID', 1, NULL, NULL));
  :lt_PAL_PARAMETER_TBL.INSERT(('VERBOSE', 0, NULL, NULL));
  :lt_PAL_PARAMETER_TBL.INSERT(('FEATURE_ATTRIBUTION_METHOD', 0,  NULL,  NULL));
  :lt_PAL_PARAMETER_TBL.INSERT(('STATE_ID', NULL, NULL, 'RDT MODEL 2025.Q4.v1')); -- reference to the custom state-ID;
  :lt_PAL_PARAMETER_TBL.INSERT(('THREAD_RATIO', NULL, 0.8, NULL));
 
  lt_data = SELECT * FROM PREDICT_DATA where ID = BIND_AS_PARAMETER(:PREDICTID);
  lt_ctrl = SELECT * FROM :lt_PAL_PARAMETER_TBL;
  lt_mdl = SELECT * FROM EMPTY_RDTMODEL;
  SELECT ( Select CASE WHEN max(RUN_ID) IS NULL THEN 0 ELSE max(RUN_ID) END FROM RUNTIME_AUDIT) INTO RUNID From Dummy;

  /* call the PAL predict-function */
  select CURRENT_TIMESTAMP INTO t1 from dummy;
  CALL _SYS_AFL.PAL_UNIFIED_CLASSIFICATION_PREDICT (:lt_data, :lt_mdl, :lt_ctrl, lt_predresult, lt_empty);
  select CURRENT_TIMESTAMP INTO t2 from dummy;

  select TO_VARCHAR(hour(:t2)-hour(:t1))||':'||TO_VARCHAR(minute(:t2)-minute(:t1))||':'||TO_VARCHAR(second(:t2)-second(:t1)) into ETIME from dummy; 
  insert into RUNTIME_AUDIT select 'PREDICT with PAL RDT stateful model: 1st call with id: '|| :PREDICTID, TO_INT(:RUNID + 1) as "RUN_ID", :t1 AS BEGIN_TIME, :t2 AS END_TIME, :ETIME as ELAPSED_TIME from dummy;
END;

truncate TABLE RUNTIME_AUDIT;

CALL PAL_FAST_PREDICT(1);
CALL PAL_FAST_PREDICT(1);
CALL PAL_FAST_PREDICT(1);
CALL PAL_FAST_PREDICT(2);
CALL PAL_FAST_PREDICT(3);
CALL PAL_FAST_PREDICT(4);
CALL PAL_FAST_PREDICT(5);
CALL PAL_FAST_PREDICT(6);

/**********************************************************************************************************************************************************************************/
/*** RESULTS  ***/
-- Results from custom runtime-audit table
select RUN_ID, TEST_NAME , BEGIN_TIME, END_TIME, ELAPSED_TIME  from RUNTIME_AUDIT;
/*
RUN_ID|TEST_NAME                                               |BEGIN_TIME             |END_TIME               |ELAPSED_TIME |
------+--------------------------------------------------------+-----------------------+-----------------------+-------------+
1     |PREDICT with PAL RDT stateful model: 1st call with id: 1|2026-01-07 18:02:18.054|2026-01-07 18:02:18.081|0:0:0.0270000|
2     |PREDICT with PAL RDT stateful model: 1st call with id: 1|2026-01-07 18:02:18.135|2026-01-07 18:02:18.156|0:0:0.0210000|
3     |PREDICT with PAL RDT stateful model: 1st call with id: 1|2026-01-07 18:02:18.195|2026-01-07 18:02:18.221|0:0:0.0260000|
4     |PREDICT with PAL RDT stateful model: 1st call with id: 2|2026-01-07 18:02:18.281|2026-01-07 18:02:18.308|0:0:0.0270000|
5     |PREDICT with PAL RDT stateful model: 1st call with id: 3|2026-01-07 18:02:18.383|2026-01-07 18:02:18.408|0:0:0.0250000|
6     |PREDICT with PAL RDT stateful model: 1st call with id: 4|2026-01-07 18:02:18.501|2026-01-07 18:02:18.525|0:0:0.0240000|
7     |PREDICT with PAL RDT stateful model: 1st call with id: 5|2026-01-07 18:02:18.597|2026-01-07 18:02:18.620|0:0:0.0230000|
8     |PREDICT with PAL RDT stateful model: 1st call with id: 6|2026-01-07 18:02:18.685|2026-01-07 18:02:18.709|0:0:0.0240000|
 */

--  Results from plan cache monitoring view after running “CALL _SYS_AFL.PAL_UNIFIED_CLASSIFICATION_PREDICT”?;
select STATEMENT_STRING, EXECUTION_ENGINE, PLAN_ID, PARAMETER_COUNT, EXECUTION_COUNT, MIN_EXECUTION_TIME, AVG_EXECUTION_TIME 
    from m_sql_plan_cache 
    where statement_string like 'CALL _SYS_AFL.PAL_UNIFIED_CLASSIFICATION_PREDICT(__table_name_parameter__(?)%' 
          AND 
         ( LAST_EXECUTION_TIMESTAMP > '2026-01-07 06:33:00.00' OR LAST_PREPARATION_TIMESTAMP > '2026-01-07 13:33:00.00');
/*
STATEMENT_STRING                                                                 |EXECUTION_ENGINE|PLAN_ID |PARAMETER_COUNT|EXECUTION_COUNT|MIN_EXECUTION_TIME|AVG_EXECUTION_TIME|
---------------------------------------------------------------------------------+----------------+--------+---------------+---------------+------------------+------------------+
CALL _SYS_AFL.PAL_UNIFIED_CLASSIFICATION_PREDICT(__table_name_parameter__(?) ... |ROW             |16040002|              5|             23|             14448|             26914|
 */


/**********************************************************************************************************************************************************************************/
/*** Appendix  ***/

/*--- Explicitly creating stateful PAL ML model object instances using  _SYS_AFL.PAL_CREATE_MODEL_STATE-----------------------------------------------------------------------------------*/
DO
BEGIN
  DECLARE lt_PAL_PARAMETER_TBL TABLE("PARAM_NAME" NVARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100));
  DECLARE lt_PAL_EMPTY_TBL TABLE ( ID  double);
  DECLARE lt_PAL_STATE_TBL TABLE(S_KEY NVARCHAR(50), S_VALUE NVARCHAR(100));

  :lt_PAL_PARAMETER_TBL.INSERT(('ALGORITHM',  2, NULL, NULL)); --2 = RDT;
  :lt_PAL_PARAMETER_TBL.INSERT(('STATE_DESCRIPTION', NULL, NULL, 'My stateful RDT model')); -- you can specify a custom statful PAL model description;  
  --:lt_PAL_PARAMETER_TBL.INSERT(('STATE_ID', NULL, NULL, 'RDT MODEL 2025.Q4.v2')); -- you can specify a custom state-ID using the STATE_ID parameter, otherwise a UUID will be generated;
  	
  lt_mdl = SELECT * FROM PAL_MODEL_RDT;
  
  CALL _SYS_AFL.PAL_CREATE_MODEL_STATE(:lt_mdl, :lt_PAL_EMPTY_TBL, :lt_PAL_EMPTY_TBL, :lt_PAL_EMPTY_TBL, :lt_PAL_EMPTY_TBL, :lt_PAL_PARAMETER_TBL, lt_PAL_STATE_TBL);
  SELECT * FROM :lt_PAL_STATE_TBL;	
END;

/*
S_KEY   |S_VALUE                             |
--------+------------------------------------+
STATE_ID|CCC980422A8E064E92F54CF268ECEB82    |
HINT    |2                                   |
HOST    |34e77cb1-06e0-4ef3-9ab9-42acaa66033e|
PORT    |30046                               | 
 */

SELECT * FROM PUBLIC.M_AFL_STATES; -- DBADMIN;
/*
|STATE_ID                        |AREA_NAME|DESCRIPTION          |CREATE_TIMESTAMP       |LAST_ACCESS_TIMESTAMP  |HOST                                |PORT  |
|--------------------------------|---------|---------------------|-----------------------|-----------------------|------------------------------------|------|
|CCC980422A8E064E92F54CF268ECEB82|AFLPAL   |My stateful RDT model|2026-01-07 17:49:29.974|2026-01-07 17:49:29.979|34e77cb1-06e0-4ef3-9ab9-42acaa66033e|30,046|

 */


/*--- Deleting stateful PAL ML model object instances using _SYS_AFL.PAL_DELETE_MODEL_STATE------------------------------------------------------------------------------------------------*/
DO
BEGIN
  DECLARE lt_PAL_PARAMETER_TBL TABLE("PARAM_NAME" NVARCHAR (100), 	"INT_VALUE" INTEGER, 	"DOUBLE_VALUE" DOUBLE, 	"STRING_VALUE" VARCHAR (100));
  DECLARE lt_PAL_STATE_TBL TABLE(S_KEY NVARCHAR(50), S_VALUE NVARCHAR(100));
  --:lt_PAL_PARAMETER_TBL.INSERT('THREAD_RATIO', NULL, 0.8, NULL);
  :lt_PAL_STATE_TBL.INSERT(('STATE_ID', 'RDT MODEL 2025.Q4.v1'));  --RDT MODEL 2025.Q4.v1
  --:lt_PAL_STATE_TBL.INSERT(('STATE_ID', 'CCC980422A8E064E92F54CF268ECEB82'));

  CALL _SYS_AFL.PAL_DELETE_MODEL_STATE(:lt_PAL_STATE_TBL, :lt_PAL_PARAMETER_TBL, lt_res);
  SELECT * FROM :lt_res ;
END;
/*
ID|TIMESTAMP|ERROR_CODE|MESSAGE|
--+---------+----------+-------+
 */




