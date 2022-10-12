CLASS zcl_islm_pal_ucl_sflight DEFINITION
  PUBLIC
  INHERITING FROM cl_hemi_model_mgmt_ucl_base
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES if_hemi_model_management.

    TYPES:
      " Data structure contains the training and prediction data
      ts_data TYPE z_sflight_train. " <<<<<< TODO: adjust the type to use the application specific fields. The fields can be casted to the appropriate data elements inside the CDS view.
    TYPES:
      " the training and prediction table types
      tt_training_data TYPE STANDARD TABLE OF ts_data WITH DEFAULT KEY,
      tt_predict_data  TYPE STANDARD TABLE OF ts_data WITH DEFAULT KEY.
    TYPES:
      " Data type contains the prediction result with application specific fields
      " Specifying the corresponding data elements or referencing CDS view fields are possible here.
      BEGIN OF ts_predict_result,
        " Fields carrid, connid, fldate are key fields. They must match the key fields of the training and prediction datasets
        carrid                   TYPE z_sflight_train-carrid,
        connid                   TYPE z_sflight_train-connid,
        fldate                   TYPE z_sflight_train-fldate,
        " Fields predict_planetype and predict_confidence are the result of prediction
        predict_planetype        TYPE z_sflight_train-planetype,
        predict_confidence       TYPE shemi_predict_confidence,
        " The number of features which contribute to classification decision most is application specific and need to be adjusted
        reason_code_feature_1    TYPE shemi_reason_code_feature_name,
        reason_code_percentage_1 TYPE shemi_reason_code_feature_pct,
        reason_code_feature_2    TYPE shemi_reason_code_feature_name,
        reason_code_percentage_2 TYPE shemi_reason_code_feature_pct,
        reason_code_feature_3    TYPE shemi_reason_code_feature_name,
        reason_code_percentage_3 TYPE shemi_reason_code_feature_pct,
      END OF ts_predict_result,
      tt_predict_result TYPE STANDARD TABLE OF ts_predict_result WITH DEFAULT KEY.

    CLASS-METHODS training
      IMPORTING
        VALUE(it_data)                TYPE tt_training_data
        VALUE(it_param)               TYPE if_hemi_model_management=>tt_pal_param
      EXPORTING
        VALUE(et_model)               TYPE cl_hemi_model_mgmt_ucl_base=>tt_model
        VALUE(et_variable_importance) TYPE shemi_variable_importance_t
        VALUE(et_confusion_matrix)    TYPE shemi_confusion_matrix_t
        VALUE(et_metrics)             TYPE if_hemi_model_management=>tt_metrics
        VALUE(et_gen_info)            TYPE if_hemi_model_management=>tt_metrics
      RAISING
        cx_amdp_execution_failed.

    CLASS-METHODS predict_with_model_version
      IMPORTING
        VALUE(it_data)   TYPE tt_predict_data
        VALUE(it_model)  TYPE cl_hemi_model_mgmt_ucl_base=>tt_model
        VALUE(it_param)  TYPE if_hemi_model_management=>tt_pal_param
      EXPORTING
        VALUE(et_result) TYPE tt_predict_result
      RAISING
        cx_amdp_execution_failed.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.

CLASS zcl_islm_pal_ucl_sflight IMPLEMENTATION.

  METHOD if_hemi_model_management~get_amdp_class_name.
    DATA lr_self TYPE REF TO zcl_islm_pal_ucl_sflight.
    TRY.
        CREATE OBJECT lr_self.
        ev_name = cl_abap_classdescr=>get_class_name( lr_self ).
      CATCH cx_badi_context_error   ##NO_HANDLER.
      CATCH cx_badi_not_implemented ##NO_HANDLER.
    ENDTRY.
  ENDMETHOD.

  METHOD if_hemi_model_management~get_meta_data.
    " the PAL model parameters for training and prediction datasets
    es_meta_data-model_parameters =
      VALUE #(
        ( name = 'FUNCTION'                  type = cl_hemi_constants=>cs_param_type-string
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )
        ( name = 'N_ESTIMATORS'              type = cl_hemi_constants=>cs_param_type-integer
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_true  has_context = abap_false )
        ( name = 'PARTITION_METHOD'          type = cl_hemi_constants=>cs_param_type-integer
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )
        ( name = 'TRAINING_PERCENT'          type = cl_hemi_constants=>cs_param_type-double
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )
        ( name = 'TESTING_PERCENT'           type = cl_hemi_constants=>cs_param_type-double
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )
        ( name = 'VALIDATION_PERCENT'        type = cl_hemi_constants=>cs_param_type-double
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )
        ( name = 'KEY'                       type = cl_hemi_constants=>cs_param_type-integer
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )
        ( name = 'COMPRESSION'               type = cl_hemi_constants=>cs_param_type-integer
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )
        ( name = 'IMPUTATION_TYPE'           type = cl_hemi_constants=>cs_param_type-integer
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )
        ( name = 'PLANETYPE_IMPUTATION_TYPE' type = cl_hemi_constants=>cs_param_type-integer
          role = cl_hemi_constants=>cs_param_role-train configurable = abap_false has_context = abap_false )

        ( name = 'FUNCTION'                  type = cl_hemi_constants=>cs_param_type-string
          role = cl_hemi_constants=>cs_param_role-apply configurable = abap_false has_context = abap_false )
        ( name = 'IMPUTATION_TYPE'           type = cl_hemi_constants=>cs_param_type-integer
          role = cl_hemi_constants=>cs_param_role-apply configurable = abap_false has_context = abap_false )
      ).

    " default values for the parameters
    es_meta_data-model_parameter_defaults =
      VALUE #(
        ( name = 'FUNCTION'                   value = 'RDT'  )
        ( name = 'PARTITION_METHOD'           value = '0'    )
        ( name = 'TRAINING_PERCENT'           value = '0.7'  )
        ( name = 'TESTING_PERCENT'            value = '0.15' )
        ( name = 'VALIDATION_PERCENT'         value = '0.15' )
        ( name = 'KEY'                        value = '1'    )
        ( name = 'COMPRESSION'                value = '1'    )
        ( name = 'IMPUTATION_TYPE'            value = '3'    ) " mode_allzero
        ( name = 'PLANETYPE_IMPUTATION_TYPE'  value = '1'    ) " delete
        ( name = 'N_ESTIMATORS'               value = '5'    )
      ).

    " training and prediction datasets are defined by CDS views
    es_meta_data-training_data_set = 'Z_SFLIGHT_TRAIN'.
    es_meta_data-apply_data_set    = 'Z_SFLIGHT_PREDICT'.

    " the target field is set to 'PLANETYPE'
    es_meta_data-field_descriptions = VALUE #( ( name = 'PLANETYPE' role = cl_hemi_constants=>cs_field_role-target ) ).
  ENDMETHOD.

  METHOD training BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.

    declare complete_statistics table (stat_name nvarchar(256), stat_value nvarchar(1000), class_name nvarchar(256));
    declare complete_metrics    table (metric_name nvarchar(256), x double, y double);
    declare validation_ki double;
    declare train_ki      double;
    declare gain_chart_point_count integer;

    /* Step 1. Input data preprocessing (missing values, rescaling, encoding, etc)
               Based on the scenario, add ids, select fields relevant for the training, cast to the appropriate data type, convert nulls into meaningful values.
               Note: decimal must be converted into double.*/

    /* <<<<<< TODO: Starting point of adaptation */
    lt_data = select to_nchar  (sysuuid)    as id,
                     to_double (price)      as price,
                     to_integer(seatsmax)   as seatsmax,
                     to_integer(seatsmax_b) as seatsmax_b,
                     to_integer(seatsocc_b) as seatsocc_b,
                     to_integer(seatsmax_f) as seatsmax_f,
                     to_integer(seatsocc_f) as seatsocc_f,
                     to_nchar  (planetype)  as planetype   -- dependent variable must be specified as the last column if it is not defined by parameter DEPENDENT_VARIABLE
                     from :it_data;
    /* <<<<<< TODO: End point of adaptation */

    call _sys_afl.pal_missing_value_handling(:lt_data, :it_param, lt_data, lt_mv_stat);

    /* Step 2. Sampling for training and model quality debriefing;
               Imbalanced up/downsampling + Train/Test/Validation random or stratified partition sampling */

    call _sys_afl.pal_partition(:lt_data, :it_param, part_id);

    lt_train = select lt.* from :lt_data as lt, :part_id as part_id where lt.id = part_id.id and part_id.partition_type = 1;
    lt_test  = select lt.* from :lt_data as lt, :part_id as part_id where lt.id = part_id.id and part_id.partition_type = 2;
    lt_val   = select lt.* from :lt_data as lt, :part_id as part_id where lt.id = part_id.id and part_id.partition_type = 3;

    /* Step 3. Unified classification training */

    call _sys_afl.pal_unified_classification(:lt_train, :it_param, :et_model, :et_variable_importance, lt_stat, lt_opt, lt_cm, lt_metrics, lt_ph1, lt_ph2);

    /* Step 4. Unified classification scoring + debriefing additional metrics and gain charts */

    call _sys_afl.pal_unified_classification_score(:lt_train, :et_model, :it_param, result_train, stats_train, cm_train,  metrics_train);
    call _sys_afl.pal_unified_classification_score(:lt_test,  :et_model, :it_param, result_test,  stats_test,  cm_test,   metrics_test);
    call _sys_afl.pal_unified_classification_score(:lt_val,   :et_model, :it_param, result_val,   stats_val,   cm_val,    metrics_val);

    -- output confusion matrix is derived from the validation dataset
    et_confusion_matrix = select * from :cm_val;

    complete_statistics = select concat('VALIDATION_', stat_name) as stat_name, stat_value, class_name from :stats_val
                union all select concat('TEST_',       stat_name) as stat_name, stat_value, class_name from :stats_test
                union all select concat('TRAIN_',      stat_name) as stat_name, stat_value, class_name from :stats_train;

    complete_metrics = select concat('VALIDATION_', "NAME") as metric_name, x, y from :metrics_val
             union all select concat('TEST_',       "NAME") as metric_name, x, y from :metrics_test
             union all select concat('TRAIN_',      "NAME") as metric_name, x, y from :metrics_train;

    -- Calculate KI and KR and other key metrics. Default values set to 0 when AUC cannot be calculated (too few rows in the dataset)
    select to_double(stat_value) * 2 - 1 into validation_ki default 0 from :complete_statistics where stat_name = 'VALIDATION_AUC';
    select to_double(stat_value) * 2 - 1 into train_ki      default 0 from :complete_statistics where stat_name = 'TRAIN_AUC';

    et_metrics = select 'PredictivePower'      as key, to_nvarchar(:validation_ki)                        as value from dummy
       union all select 'PredictionConfidence' as key, to_nvarchar(1.0 - abs(:validation_ki - :train_ki)) as value from dummy
    -- Provide metrics that are displayed in the quality information section of a model version in the ISLM Intelligent Scenario Management app
    /* <<<<<< TODO: Starting point of adaptation */
       union all select 'AUC'                  as key, stat_value                                         as value from :complete_statistics
                    where stat_name = 'VALIDATION_AUC';
    /* <<<<<< TODO: End point of adaptation */

    gain_chart = select general.x,
                        (select min(y) from :complete_metrics as train_col      where metric_name = 'TRAIN_CUMGAINS'      and general.x = train_col.x     ) as train,
                        (select min(y) from :complete_metrics as validation_col where metric_name = 'VALIDATION_CUMGAINS' and general.x = validation_col.x) as validation,
                        (select min(y) from :complete_metrics as test_col       where metric_name = 'TEST_CUMGAINS'       and general.x = test_col.x      ) as test,
                        (select min(y) from :complete_metrics as wizard_col     where metric_name = 'TRAIN_PERF_CUMGAINS' and general.x = wizard_col.x    ) as wizard
                 from :complete_metrics as general where general.metric_name like_regexpr '(TRAIN|VALIDATION|TEST)_CUMGAINS' group by general.x order by general.x asc;

    gain_chart = select t1.x, t1.train, t1.validation, t1.test, coalesce(t1.wizard, t2.wizard) as wizard from :gain_chart as t1
        left outer join :gain_chart as t2 on t2.x = ( select max(t3.x) from :gain_chart as t3 where t3.x < t1.x and t3.wizard is not null);
    gain_chart = select t1.x, coalesce(t1.train, t2.train) as train, t1.validation, t1.test, t1.wizard from :gain_chart as t1
        left outer join :gain_chart as t2 on t2.x = ( select max(t3.x) from :gain_chart as t3 where t3.x < t1.x and t3.train is not null);
    gain_chart = select t1.x, t1.train, coalesce(t1.validation, t2.validation) as validation, t1.test, t1.wizard from :gain_chart as t1
        left outer join :gain_chart as t2 on t2.x = ( select max(t3.x) from :gain_chart as t3 where t3.x < t1.x and t3.validation is not null);
    gain_chart = select t1.x, t1.train, t1.validation, coalesce(t1.test, t2.test) as test, t1.wizard from :gain_chart as t1
        left outer join :gain_chart as t2 on t2.x = ( select max(t3.x) from :gain_chart as t3 where t3.x < t1.x and t3.TEST is not null) order by x;

    select count(x) into gain_chart_point_count from :gain_chart;

    et_gen_info = select 'HEMI_Profitcurve' as key,
                         '{ "Type": "detected", "Frequency" : "' || round(x * :gain_chart_point_count, 0, round_down) / :gain_chart_point_count || '", "Random" : "' || round(x * :gain_chart_point_count, 0, round_down) / :gain_chart_point_count
                         || '", "Wizard": "' || wizard || '", "Estimation": "' || train || '", "Validation": "' || validation || '", "Test": "' || test || '"}' as value
                         from :gain_chart
    -- Provide metrics that are displayed in the general additional info section of a model version in the ISLM Intelligent Scenario Management app
    /* <<<<<< TODO: Starting point of adaptation */
        union all select stat_name as key, stat_value as value from :complete_statistics where class_name is null;
    /* <<<<<< TODO: End point of adaptation */
  ENDMETHOD.

  METHOD predict_with_model_version BY DATABASE PROCEDURE FOR HDB LANGUAGE SQLSCRIPT OPTIONS READ-ONLY.
    /* Step 1. Input data preprocessing (missing values, rescaling, encoding, etc).
               Note: the input data preprocessing must correspond with the one in the training method.
               Based on the scenario, add ids, select fields relevant for the training, cast to the appropriate data type, convert nulls into meaningful values.
               Note: decimal must be converted into double. */
    /* <<<<<< TODO: Starting point of adaptation */
    -- The pal_unified_classification_predict method accepts only one key field which is generated for each row of input data as an UUID.
    lt_data = select to_nchar(sysuuid) as id, carrid, connid, fldate, to_double(price) as price, seatsmax, seatsmax_b, seatsocc_b, seatsmax_f, seatsocc_f from :it_data;
    -- Only the new key and features are used in prediction.
    lt_data_predict = select                           id,
                             to_double (price     ) as price,
                             to_integer(seatsmax  ) as seatsmax,
                             to_integer(seatsmax_b) as seatsmax_b,
                             to_integer(seatsocc_b) as seatsocc_b,
                             to_integer(seatsmax_f) as seatsmax_f,
                             to_integer(seatsocc_f) as seatsocc_f
                             from :lt_data;
    /* <<<<<< TODO: End point of adaptation */

    call _sys_afl.pal_missing_value_handling(:lt_data_predict, :it_param, lt_data_predict, lt_placeholder1);

    /* Step 2. Execute prediction */

    call _sys_afl.pal_unified_classification_predict(:lt_data_predict, :it_model, :it_param, lt_result, lt_placeholder2);

    /* Step 3. Map prediction results back to the composite key */

    /* <<<<<< TODO: Starting point of adaptation */
    -- application specific key fields are carrid, connid, fldate
    et_result = select data.carrid,
                       data.connid,
                       data.fldate,
                       cast(result.score as "$ABAP.type( shemi_planetye )")            as predict_planetype,
                       result.confidence                                               as predict_confidence,
                       trim(both '"' from json_query(result.reason_code, '$[0].attr')) as reason_code_feature_1,
                       json_query(result.reason_code, '$[0].pct' )                     as reason_code_percentage_1,
                       trim(both '"' from json_query(result.reason_code, '$[1].attr')) as reason_code_feature_2,
                       json_query(result.reason_code, '$[1].pct' )                     as reason_code_percentage_2,
                       trim(both '"' from json_query(result.reason_code, '$[2].attr')) as reason_code_feature_3,
                       json_query(result.reason_code, '$[2].pct' )                     as reason_code_percentage_3
                       from :lt_data as data inner join :lt_result as result on data.id = result.id;
    /* <<<<<< TODO: End point of adaptation */
  ENDMETHOD.

ENDCLASS.
