"""
This module handles the generation of the files that represent the
artifacts. Currently this is experimental code only.
"""
import os
import json

from .filewriter_base import FileWriterBase

from ...sql_processor import SqlProcessor
from ...hana_ml_utils import StringUtils
from ...config import ConfigConstants


class AMDPWriter(FileWriterBase):
    """
    This class generates a amdp file
    """
    def generate(self, path, input, model_interface_name, output, summary_tables, algo, body):
        """
        Generate the amdp file
        """
        self.write_file(path, input, model_interface_name, output, summary_tables, algo, body)

    def write_file(self, path, input, model_interface_name, output, summary_tables, algo, call_statement):
        """
        Write the amdp file
        """
        template_file_path = os.path.join(os.path.dirname(__file__), ConfigConstants.TEMPLATE_DIR, ConfigConstants.AMDP_TEMPLATE_FILENAME)
        schema_name = self.config.get_entry(ConfigConstants.CONFIG_KEY_SCHEMA)
        app_id = self.config.get_entry(ConfigConstants.CONFIG_KEY_APPID)
        abap_in_type = input['abap_type']
        abap_out_type = output['abap_type']
        in_tab_name = input['interface_name']
        out_tab_name = output['interface_name']

        # Set replacement variables according to function type (derived from output table)
        if self.config.is_model_category(output['cat']):
            out_cat = 'MODEL'
            function_type = 'TRAIN'
            using = ''
            model_select = ''
            function_name = 'train_model'
            declarations = summary_tables.replace('out', 'DECLARE').replace('), D',');\n D')+';'

        elif self.config.is_fitted_category(output['cat']):
            out_cat = 'RESULT'
            function_type = 'APPLY'
            using = 'USING <<MODEL_DSO>>'
            model_select = 'lt_model_table = SELECT * FROM <<MODEL_DSO>>'
            function_name = 'apply_model'
            declarations = ''
        else:
            pass

        amdp_name = app_id + '_' + algo + '_' + function_type

        # Replace objects in call statement with the tables specified in AMDP
        call_statement = call_statement.replace('CALL ', 'CALL '+schema_name+'.')
        call_statement = call_statement.replace(':'+in_tab_name, ':lt_'+algo+'_'+function_type+'_DATA')
        call_statement = call_statement.replace('> '+out_tab_name, '> lt_'+algo+'_'+out_cat)
        call_statement = call_statement.replace(':'+model_interface_name, ':lt_model_table')

        replacements = {
            ConfigConstants.AMDP_TEMPLATE_AMDP_NAME_PLACEHOLDER: amdp_name,
            ConfigConstants.AMDP_TEMPLATE_ALGORITHM_PLACEHOLDER: algo,
            ConfigConstants.AMDP_TEMPLATE_INPUT_SIGNATURE_PLACEHOLDER: abap_in_type,
            ConfigConstants.AMDP_TEMPLATE_OUTPUT_SIGNATURE_PLACEHOLDER: abap_out_type,
            ConfigConstants.AMDP_TEMPLATE_FUNCTION_NAME_PLACEHOLDER: function_name,
            ConfigConstants.AMDP_TEMPLATE_FUNCTION_TYPE_PLACEHOLDER: function_type,
            ConfigConstants.AMDP_TEMPLATE_OUTPUT_PLACEHOLDER: out_cat,
            ConfigConstants.AMDP_TEMPLATE_USING_STATEMENT_PLACEHOLDER: using,
            ConfigConstants.AMDP_TEMPLATE_MODEL_SELECT_PLACEHOLDER: model_select,
            ConfigConstants.AMDP_TEMPLATE_CALL_STATEMENT_PLACEHOLDER: call_statement,
            ConfigConstants.AMDP_TEMPLATE_TABLE_DECLARATIONS_PLACEHOLDER: declarations
        }
        file_name = algo+'_'+function_type+ConfigConstants.AMDP_FILE_EXTENSION

        self.write_template(path, file_name, template_file_path, replacements)
