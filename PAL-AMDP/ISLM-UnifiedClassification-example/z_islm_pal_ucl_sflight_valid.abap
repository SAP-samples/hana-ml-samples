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
    ASSERT cl_abap_classdescr=>get_class_name( lr_rdt_template ) = lv_amdp_class_name.

    " getting metadata of class via get_meta_data
    CALL METHOD lr_template_object->if_hemi_model_management~get_meta_data
      RECEIVING
        es_meta_data = DATA(ls_meta_data).
    WRITE: 'Training and apply datasets:', ls_meta_data-training_data_set, ls_meta_data-apply_data_set.
    SKIP.
    LOOP AT ls_meta_data-model_parameters ASSIGNING FIELD-SYMBOL(<fs_model_parameter>).
      WRITE: 'Parameter:', <fs_model_parameter>-name, <fs_model_parameter>-type, <fs_model_parameter>-role, <fs_model_parameter>-configurable, <fs_model_parameter>-has_context, <fs_model_parameter>-obligatory.
      SKIP.
    ENDLOOP.
    LOOP AT ls_meta_data-model_parameter_defaults ASSIGNING FIELD-SYMBOL(<fs_model_parameter_default>).
      WRITE: 'Parameter default value:', <fs_model_parameter_default>-name, <fs_model_parameter_default>-context, <fs_model_parameter_default>-value.
      SKIP.
    ENDLOOP.
    LOOP AT ls_meta_data-field_descriptions ASSIGNING FIELD-SYMBOL(<fs_field_description>).
      WRITE: 'Field description:', <fs_field_description>-name, <fs_field_description>-role.
      SKIP.
    ENDLOOP.

    " obtaining training and apply procedure parameters
    CALL METHOD lr_template_object->if_hemi_procedure~get_procedure_parameters
      IMPORTING
        et_training = DATA(lt_training)
        et_apply    = DATA(lt_apply).
    LOOP AT lt_training ASSIGNING FIELD-SYMBOL(<fs_training>).
      WRITE: 'Training procedure parameter:', <fs_training>-name, <fs_training>-role, <fs_training>-add_info.
      SKIP.
    ENDLOOP.
    LOOP AT lt_apply ASSIGNING FIELD-SYMBOL(<fs_apply>).
      WRITE: 'Apply procedure parameter:', <fs_apply>-name, <fs_apply>-role, <fs_apply>-add_info.
      SKIP.
    ENDLOOP.

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

    " calling training method
    CALL METHOD lr_template_object->training
      EXPORTING
        it_data                = lt_training_data
        it_param               = lt_training_param
      IMPORTING
        et_model               = DATA(lt_model)
        et_variable_importance = DATA(lt_variable_importance).
    LOOP AT lt_variable_importance ASSIGNING FIELD-SYMBOL(<fs_variable_importance>).
      WRITE: 'Variable importance:', <fs_variable_importance>-variable_name, <fs_variable_importance>-importance.
      SKIP.
    ENDLOOP.

    " calling prediction method
    CALL METHOD lr_template_object->predict_with_model_version
      EXPORTING
        it_data   = lt_predict_data
        it_model  = lt_model
        it_param  = lt_predict_param
      IMPORTING
        et_result = DATA(lt_result).
    LOOP AT lt_result ASSIGNING FIELD-SYMBOL(<fs_result>).
      WRITE: 'Prediction result:', <fs_result>-carrid, <fs_result>-connid, <fs_result>-fldate, <fs_result>-predict_planetype, <fs_result>-predict_confidence.
      SKIP.
    ENDLOOP.

  CATCH cx_amdp_execution_failed INTO lr_amdp_exp.
    BREAK-POINT ##NO_BREAK.
    WRITE: lr_amdp_exp->get_text( ).
ENDTRY.
