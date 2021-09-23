REPORT z_islm_pal_ucl_sflight_valid.

DATA: lr_template_object TYPE REF TO zcl_islm_pal_ucl_sflight,

      ls_training_data   TYPE lr_template_object->ts_data,
      ls_predict_data    TYPE lr_template_object->ts_data,
      lt_training_data   TYPE lr_template_object->tt_training_data,
      lt_predict_data    TYPE lr_template_object->tt_predict_data,

      ls_param           TYPE if_hemi_model_management=>ts_pal_param,
      lt_training_param  TYPE if_hemi_model_management=>tt_pal_param,
      lt_predict_param   TYPE if_hemi_model_management=>tt_pal_param,

      lr_amdp_exp        TYPE REF TO cx_amdp_execution_failed.

TRY.
    CREATE OBJECT lr_template_object.

    " getting class name via get_amdp_class_name
    CALL METHOD lr_template_object->if_hemi_model_management~get_amdp_class_name
      RECEIVING
        ev_name = DATA(lv_amdp_class_name).
    WRITE: 'AMDP class name:', lv_amdp_class_name.
    SKIP.
    " the AMDP class name and the class name of reference lr_self in if_hemi_model_management~get_amdp_class_name() must be the same
    ASSERT cl_abap_classdescr=>get_class_name( lr_template_object ) = lv_amdp_class_name.

    " getting metadata of class via get_meta_data
    CALL METHOD lr_template_object->if_hemi_model_management~get_meta_data
      RECEIVING
        es_meta_data = DATA(ls_meta_data).
    WRITE: 'Training and apply datasets:', ls_meta_data-training_data_set, ls_meta_data-apply_data_set.
    SKIP.
    PERFORM print_table USING 'Parameter:' ls_meta_data-model_parameters.
    PERFORM print_table USING 'Parameter default value:' ls_meta_data-model_parameter_defaults.
    PERFORM print_table USING 'Field description:' ls_meta_data-field_descriptions.

    " obtaining training and apply procedure parameters
    CALL METHOD lr_template_object->if_hemi_procedure~get_procedure_parameters
      IMPORTING
        et_training = DATA(lt_training)
        et_apply    = DATA(lt_apply).
    PERFORM print_table USING 'Training procedure parameter:' lt_training.
    PERFORM print_table USING 'Apply procedure parameter:' lt_apply.

    " constructing training parameters from AMDP class
    LOOP AT ls_meta_data-model_parameters ASSIGNING FIELD-SYMBOL(<ls_model_parameter>).
      READ TABLE ls_meta_data-model_parameter_defaults WITH KEY name = <ls_model_parameter>-name ASSIGNING FIELD-SYMBOL(<ls_model_parameter_default>).
      ASSERT sy-subrc = 0.
      CLEAR ls_param.
      ls_param-name = <ls_model_parameter>-name.
      CASE <ls_model_parameter>-type.
        WHEN cl_hemi_constants=>cs_param_type-integer.
          ls_param-intargs = <ls_model_parameter_default>-value.
        WHEN cl_hemi_constants=>cs_param_type-double.
          ls_param-doubleargs = <ls_model_parameter_default>-value.
        WHEN cl_hemi_constants=>cs_param_type-string.
          ls_param-stringargs = <ls_model_parameter_default>-value.
        WHEN OTHERS.
          CONTINUE.
      ENDCASE.
      IF <ls_model_parameter>-role = cl_hemi_constants=>cs_param_role-train.
        APPEND ls_param TO lt_training_param.
      ELSEIF <ls_model_parameter>-role = cl_hemi_constants=>cs_param_role-apply.
        APPEND ls_param TO lt_predict_param.
      ENDIF.
    ENDLOOP.

    " checking training and apply dataset strings for SQL injection
    TRY.
        DATA(lv_training_cds_view_name) = cl_abap_dyn_prg=>check_table_name_str( packages = '' val = ls_meta_data-training_data_set ).
      CATCH cx_abap_not_a_table.
        WRITE: 'String for training dataset: ', ls_meta_data-training_data_set, ' is not valid name'.
        RETURN.
      CATCH cx_abap_not_in_package.
        RETURN.
    ENDTRY.
    TRY.
        DATA(lv_predict_cds_view_name)  = cl_abap_dyn_prg=>check_table_name_str( packages = '' val = ls_meta_data-apply_data_set ).
      CATCH cx_abap_not_a_table.
        WRITE: 'String for apply dataset: ', ls_meta_data-apply_data_set, ' is not valid name'.
        RETURN.
      CATCH cx_abap_not_in_package.
        RETURN.
    ENDTRY.

    " retrieving training and apply data from CDS views
    SELECT * INTO TABLE @lt_training_data FROM (lv_training_cds_view_name).
    SELECT * INTO TABLE @lt_predict_data  FROM (lv_predict_cds_view_name).

    DATA: lv_time_start   TYPE i,
          lv_time_finish  TYPE i,
          lv_time_elapsed TYPE i.

    " calling training method

    GET RUN TIME FIELD lv_time_start.
    CALL METHOD lr_template_object->training
      EXPORTING
        it_data                = lt_training_data
        it_param               = lt_training_param
      IMPORTING
        et_model               = DATA(lt_model)
        et_variable_importance = DATA(lt_variable_importance).
    GET RUN TIME FIELD lv_time_finish.

    lv_time_elapsed = lv_time_finish - lv_time_start.
    WRITE:/ 'Training Runtime =', lv_time_elapsed, 'microseconds'.
    SKIP.
    PERFORM print_table USING 'Variable importance:' lt_variable_importance.

    " calling prediction method

    GET RUN TIME FIELD lv_time_start.
    CALL METHOD lr_template_object->predict_with_model_version
      EXPORTING
        it_data   = lt_predict_data
        it_model  = lt_model
        it_param  = lt_predict_param
      IMPORTING
        et_result = DATA(lt_result).
    GET RUN TIME FIELD lv_time_finish.

    lv_time_elapsed = lv_time_finish - lv_time_start.
    WRITE:/ 'Prediction Runtime =', lv_time_elapsed, 'microseconds'.
    SKIP.
    PERFORM print_table USING 'Prediction result:' lt_result.

  CATCH cx_amdp_execution_failed INTO lr_amdp_exp.
    BREAK-POINT ##NO_BREAK.
    WRITE: lr_amdp_exp->get_text( ).
ENDTRY.

FORM print_table
  USING
    iv_label TYPE string
    iv_table TYPE ANY TABLE.
  WRITE: iv_label.
  SKIP.
  LOOP AT iv_table ASSIGNING FIELD-SYMBOL(<ls_table>).
    DO.
      ASSIGN COMPONENT sy-index OF STRUCTURE <ls_table> TO FIELD-SYMBOL(<ls_field>).
      IF sy-subrc EQ 0.
        WRITE <ls_field>.
      ELSE.
        EXIT.
      ENDIF.
    ENDDO.
    SKIP.
  ENDLOOP.
  SKIP.
ENDFORM.