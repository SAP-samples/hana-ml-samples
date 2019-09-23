"""
This module handles generation of all AMDP related artifacts based on the provided
consumption layer elements. Currently this is experimental code only.
"""
import os 

from .filewriter.abap import AMDPWriter

from ..config import ConfigConstants
from ..hana_ml_utils import DirectoryHandler
from ..hana_ml_utils import StringUtils

from ..sql_processor import SqlProcessor

from .hana import HanaGeneratorHelper


class AMDPGenerator(object):
    """
    This class provides AMDP specific generation functionality. It also extend the config
    to cater for AMDP generation specific config.
    """
    def __init__(self, config):
        """
        This is main entry point for generating the AMDP related artifacts.

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.hana_helper = HanaGeneratorHelper(config)
        self.directory_handler = DirectoryHandler()
        self.config = config
        self._extend_config()

    def _build_folder_structure(self):
        """
        Build up the folder structure. It is currenlty not a deep structure but just a subbfolder abap
        under the root output path.
        """
        self._clean_folder_structurre()
        # Create base directories
        self.directory_handler.create_directory( self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_ABAP ))
        
    def _clean_folder_structurre(self):
        """
        Clean up physical folder structure. 
        """
        path = self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_ABAP )
        if os.path.exists( path ):
            self.directory_handler.delete_directory_content( path )
            os.rmdir( path )

    def _extend_config(self):
        """
        Extend the config to cater for AMDP generation specific config.
        """  
        output_path_amdp = os.path.join(self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH ), ConfigConstants.ABAP_BASE_PATH )
        self.config.add_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_ABAP, output_path_amdp)

    def generate(self, consumption_layer=True):
        """
        Generate the artifacts by first building up the required folder structure for artifact storage and then 
        generating the different required files. 

        Parameters
        ----------
        consumption_layer : boolean
            The consumption layer is the layer that will consume the base layer artifacts
        
        """
        self._build_folder_structure()
        
        amdp_writer = AMDPWriter(self.config)
        sql_key_sql = SqlProcessor.TRACE_KEY_SQL_PROCESSED

        if consumption_layer:
            sql_processed_cons_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[SqlProcessor.TRACE_KEY_CONSUMPTION_LAYER]

            for element in sql_processed_cons_layer:
                if not isinstance(element, dict):
                    continue # ignore TODO: proper doc
                if element['groups'][0]['type'] in {'fit', 'predict'}:
                    if sql_key_sql in element: # TODO: gen warning if no sql
                        summary_tables = []

                        if 'output' in element[sql_key_sql]:
                            for table in element[sql_key_sql]['output']:
                                if (self.config.is_model_category(table['cat']) or self.config.is_fitted_category(table['cat'] )):
                                    out_cat = table['cat']
                                    output = table                                  # Only one output allowed in transformation context
                                else:
                                    summary_tables.append(table)                    # Store model debrief tables for DECLARE statements
                        
                        if 'input' in element[sql_key_sql]:
                            for table in element[sql_key_sql]['input']:
                                if (self.config.is_model_category(table['cat']) == False):
                                    input = table                                   # Only one input allowed in transformation context
                                    model_interface_name = ''
                                else:
                                    model_interface_name = table['interface_name'] 

                        if 'body' in element[sql_key_sql]: 
                            algo = element['algo']
                            item = element[sql_key_sql]['body'][0]                   #Intermediate step for readability of next line
                            body = item[sql_key_sql].format(*item['sql_vars'])       

                        signature_str = self.hana_helper._build_procedure_signature(None, summary_tables) # only model debrief tables
                        amdp_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_ABAP), input, model_interface_name, output, signature_str, algo, body)
