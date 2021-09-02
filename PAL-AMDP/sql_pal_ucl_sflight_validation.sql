-- <sap_schema> and <sap_client> have to be replaced by appropriate values

SELECT * FROM <sap_schema>.SFLIGHT WHERE MANDT = '<sap_client>' ORDER BY FLDATE;

-- Creating the training and prediction views

DROP VIEW SFLIGHT_TRAIN_SQL;
CREATE VIEW SFLIGHT_TRAIN_SQL AS
  SELECT * FROM <sap_schema>.Z_SFLI_TRAIN WHERE MANDT = '<sap_client>';

SELECT * FROM SFLIGHT_TRAIN_SQL;

DROP VIEW SFLIGHT_PREDICT_SQL;
CREATE VIEW SFLIGHT_PREDICT_SQL AS
  SELECT * FROM <sap_schema>.Z_SFLI_PRED WHERE MANDT = '<sap_client>';

SELECT * FROM SFLIGHT_PREDICT_SQL;

----------------------------------- Training (Unified Classification) ---------------------------------

-- Training parameters
DROP TABLE PAL_PARAMETER_TBL;
CREATE TABLE PAL_PARAMETER_TBL (
    "PARAM_NAME" VARCHAR (100),
    "INT_VALUE" INTEGER,
    "DOUBLE_VALUE" DOUBLE,
    "STRING_VALUE" VARCHAR (100)
);
INSERT INTO PAL_PARAMETER_TBL VALUES ('FUNCTION', NULL, NULL, 'RDT');
INSERT INTO PAL_PARAMETER_TBL VALUES ('PARTITION_METHOD', 0, NULL, NULL);
INSERT INTO PAL_PARAMETER_TBL VALUES ('TRAINING_PERCENT', NULL, 0.7, NULL);
INSERT INTO PAL_PARAMETER_TBL VALUES ('TESTING_PERCENT', NULL, 0.15, NULL);
INSERT INTO PAL_PARAMETER_TBL VALUES ('VALIDATION_PERCENT', NULL, 0.15, NULL);
INSERT INTO PAL_PARAMETER_TBL VALUES ('KEY', 1, NULL, NULL);
INSERT INTO PAL_PARAMETER_TBL VALUES ('COMPRESSION', 1, NULL, NULL);
INSERT INTO PAL_PARAMETER_TBL VALUES ('IMPUTATION_TYPE', 3, NULL, NULL);
INSERT INTO PAL_PARAMETER_TBL VALUES ('PLANETYPE_IMPUTATION_TYPE', 1, NULL, NULL);
INSERT INTO PAL_PARAMETER_TBL VALUES ('N_ESTIMATORS', 5, NULL, NULL);

-- Model
DROP TABLE PAL_MODEL_TBL;
CREATE COLUMN TABLE PAL_MODEL_TBL (
    "ROW_INDEX" INTEGER,
    "PART_INDEX" INTEGER,
    "MODEL_CONTENT" NVARCHAR(5000)
);

-- Executing training and retrieving the model and statistics
DO
BEGIN
    lt_data = select to_nchar  (sysuuid)    as id,
                     to_double (price)      as price,
                     to_integer(seatsmax)   as seatsmax,
                     to_integer(seatsmax_b) as seatsmax_b,
                     to_integer(seatsocc_b) as seatsocc_b,
                     to_integer(seatsmax_f) as seatsmax_f,
                     to_integer(seatsocc_f) as seatsocc_f,
                     to_nchar  (planetype)  as planetype
                     from sflight_train_sql;

    lt_param = select * from pal_parameter_tbl;

    call _sys_afl.pal_missing_value_handling(:lt_data, :lt_param, lt_data, lt_mv_stat);
    call _sys_afl.pal_partition(:lt_data, :lt_param, part_id);

    lt_train = select lt.* from :lt_data as lt, :part_id as part_id where lt.id = part_id.id and part_id.partition_type = 1;
    lt_test  = select lt.* from :lt_data as lt, :part_id as part_id where lt.id = part_id.id and part_id.partition_type = 2;
    lt_val   = select lt.* from :lt_data as lt, :part_id as part_id where lt.id = part_id.id and part_id.partition_type = 3;

    /* execute PAL training */
    call _sys_afl.pal_unified_classification(:lt_train, :lt_param, lt_model, lt_variable_importance, lt_stat, lt_opt, lt_cm, lt_metrics, lt_ph1, lt_ph2);

    call _sys_afl.pal_unified_classification_score(:lt_train, :lt_model, :lt_param, result_train, stats_train, cm_train,  metrics_train);

    insert into pal_model_tbl select * from :lt_model;
    select * from :lt_variable_importance order by importance desc;
    select * from :cm_train;
    select * from :metrics_train;
END;

SELECT * FROM PAL_MODEL_TBL;

---------------------------------------- Prediction (Unified Classification) ---------------------------------------------

TRUNCATE TABLE PAL_PARAMETER_TBL;
INSERT INTO PAL_PARAMETER_TBL VALUES ('FUNCTION', NULL, NULL, 'RDT');
INSERT INTO PAL_PARAMETER_TBL VALUES ('IMPUTATION_TYPE', 3, NULL, NULL);

-- Prediction output table
DROP TABLE RESULT;
CREATE TABLE RESULT (
    "CARRID" VARCHAR(3),
    "CONNID" VARCHAR(4),
    "FLDATE" VARCHAR(8),
    "PREDICT_PLANETYPE" VARCHAR(100),
    "PREDICT_CONFIDENCE" DOUBLE,
    "REASON_CODE_FEATURE_1" varchar(256),
    "REASON_CODE_PERCENTAGE_1" double,
    "REASON_CODE_FEATURE_2" varchar(256),
    "REASON_CODE_PERCENTAGE_2" double,
    "REASON_CODE_FEATURE_3" varchar(256),
    "REASON_CODE_PERCENTAGE_3" double
);

-- Executing prediction and retrieving the prediction output
DO
BEGIN
  lt_data = select to_nchar(sysuuid) as id, * from sflight_predict_sql;

  lt_data_predict = select                            id,
                            to_double (price     ) as price,
                            to_integer(seatsmax  ) as seatsmax,
                            to_integer(seatsmax_b) as seatsmax_b,
                            to_integer(seatsocc_b) as seatsocc_b,
                            to_integer(seatsmax_f) as seatsmax_f,
                            to_integer(seatsocc_f) as seatsocc_f
                    from :lt_data;

  lt_param = select * from pal_parameter_tbl;

  call _sys_afl.pal_missing_value_handling(:lt_data_predict, :lt_param, lt_data_predict, lt_placeholder1);

  lt_model = select * from pal_model_tbl;

  /* execute PAL prediction */
  call _sys_afl.pal_unified_classification_predict(:lt_data_predict, :lt_model, :lt_param, lt_result, lt_placeholder2);

  /* prediction results are mapped back to the composite key (carrid, connid, fldate) */
  insert into result select data.carrid,
                            data.connid,
                            data.fldate,
                            result.score                                                    as predict_planetype,
                            result.confidence                                               as predict_confidence,
                            trim(both '"' from json_query(result.reason_code, '$[0].attr')) as reason_code_feature_1,
                            json_query(result.reason_code, '$[0].pct' )                     as reason_code_percentage_1,
                            trim(both '"' from json_query(result.reason_code, '$[1].attr')) as reason_code_feature_2,
                            json_query(result.reason_code, '$[1].pct' )                     as reason_code_percentage_2,
                            trim(both '"' from json_query(result.reason_code, '$[2].attr')) as reason_code_feature_3,
                            json_query(result.reason_code, '$[2].pct' )                     as reason_code_percentage_3
                            from :lt_data as data inner join :lt_result as result on data.id = result.id;
END;

SELECT * FROM RESULT;
