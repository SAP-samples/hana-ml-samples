"""
This module handles generation of all HANA related artifacts based on the provided
base and consumption layer elements.
"""
import os 

from .filewriter.hana import MTAYamlWriter
from .filewriter.hana import HDBSynonymWriter
from .filewriter.hana import HDBGrantWriter
from .filewriter.hana import HDBRoleWriter 
from .filewriter.hana import HDBProcedureWriter
from .filewriter.hana import HDBVirtualTableWriter
from .filewriter.hana import HDBCDSWriter

from ..config import ConfigConstants
from ..hana_ml_utils import DirectoryHandler
from ..hana_ml_utils import StringUtils

from ..sql_processor import SqlProcessor

class HanaGenerator(object):
    """
    This class provides HANA specific generation functionality. It also extend the config
    to cater for HANA generation specific config.
    """
    def __init__(self, config):
        """
        This is main entry point for generating the HANA related artifacts.

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.hana_helper = HanaGeneratorHelper(config)
        self.config = config
        self._extend_config()

    def generate_artifacts(self, base_layer=True, consumption_layer=True, sda_data_source_mapping_only=False):
        """
        Generate the artifacts by first building up the required folder structure for artifact storage and then 
        generating the different required files. Be aware that this method only generates the generic files
        and offloads the generation of artifacts where traversal of base and consumption layer 
        elements is required. 

        Parameters
        ----------
        base_layer : boolean
            The base layer is the low level procedures that will be generated.
        consumption_layer : boolean
            The consumption layer is the layer that will consume the base layer artifacts
        sda_data_source_mapping_only: boolean
            In case data source mapping is provided you can forrce to only do this for the
            sda hdi container
        
        Returns
        -------
        output_path : str
            Return the output path of the root folder where the hana related artifacts are stored.
        """
        self.hana_helper._build_folder_structure( ConfigConstants.PROJECT_TEMPLATE_BASE_STRUCT, self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_HANA ), self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_MODULE ))

        # Instantiate file writers
        yaml_writer = MTAYamlWriter(self.config)
        grant_writer = HDBGrantWriter(self.config)
        synonym_writer = HDBSynonymWriter(self.config)
        role_writer = HDBRoleWriter(self.config)
        consumption_processor = HanaConsumptionProcessor(self.config)

        # Generate mta yaml file
        output_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_HANA)
        app_id = self.config.get_entry( ConfigConstants.CONFIG_KEY_APPID )
        module_name = self.config.get_entry( ConfigConstants.CONFIG_KEY_MODULE_NAME )
        version = self.config.get_entry( ConfigConstants.CONFIG_KEY_VERSION )
        schema = self.config.get_entry( ConfigConstants.CONFIG_KEY_SCHEMA )
        grant_service =self.config.get_entry( ConfigConstants.CONFIG_KEY_GRANT_SERVICE )
        yaml_writer.generate(output_path, app_id, module_name, version, schema, grant_service)

        # Generate hdbgrants
        grant_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_GRANTS_PATH))

        # Generate dataset/function synonym
        synonym_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SYNONYMS_PATH)) 
 
        # Generate hdbroles for external access
        role_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_ROLES_PATH), name=self.config.get_entry(ConfigConstants.CONFIG_KEY_MODULE_NAME))

        # Generate Consumption Artifacts
        consumption_processor.generate( base_layer, consumption_layer, sda_data_source_mapping_only)

        return output_path

    def _extend_config(self):
        """
        Extend the config to cater for HANA generation specific config.
        """
        # Split made between project_name, module_name, app_id as these are seperate entries. However for now set as project_name
        # This allows more finegrained control of these config items in future releases if required. 
        module_name = self.config.get_entry( ConfigConstants.CONFIG_KEY_MODULE_NAME )
        
        # Specific folder locations for the different category of artifacts.
        output_path_hana = os.path.join(self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH ), ConfigConstants.HANA_BASE_PATH )
        output_path_module = os.path.join(output_path_hana, module_name)
        output_path_module_src = os.path.join(output_path_module, ConfigConstants.MODULE_SOURCE_PATH)
        module_template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ConfigConstants.PROJECT_TEMPLATEDR)
        grants_path = os.path.join(output_path_module_src, ConfigConstants.GRANTS_SOURCE_PATH)
        synonyms_path = os.path.join(output_path_module_src, ConfigConstants.SYNONYMS_SOURCE_PATH)
        procedures_path = os.path.join(output_path_module_src, ConfigConstants.PROCEDURES_SOURCE_PATH)
        roles_path = os.path.join(output_path_module_src, ConfigConstants.ROLES_SOURCE_PATH)
        cds_path = os.path.join(output_path_module_src, ConfigConstants.CDS_SOURCE_PATH)

        self.config.add_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_HANA, output_path_hana)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_MODULE, output_path_module)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_MODULE_SRC, output_path_module_src)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_MODULE_TEMPLATE_PATH, module_template_path)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_GRANTS_PATH, grants_path) 
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SYNONYMS_PATH, synonyms_path)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_PROCEDURES_PATH, procedures_path)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_ROLES_PATH, roles_path)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_CDS_PATH, cds_path)


class HanaConsumptionProcessor(object):
    """
    This class provides HANA specific generation functionality for the base and consumption layer. 
    It generates the files for which traversal of the base and comptiontion layer elements is
    required. The actual generation of the base layer is initiated here but is offloaded to the
    HanaGeneratorHelper class for reusability and logic seperation.
    """
    def __init__(self, config):
        """
        This class allow to generate the arifacts for the base and consumption layer. 

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.hana_helper = HanaGeneratorHelper(config)
        self.config = config

    def generate(self, base_layer=True, consumption_layer=True, sda_data_source_mapping_only=False):
        """
        Method for generating the actual artifacts content.  

        Parameters
        ----------
        base_layer : boolean
            The base layer is the low level procedures that will be generated.
        consumption_layer : boolean
            The consumption layer is the layer that will consume the base layer artifacts
        sda_data_source_mapping_only: boolean
            In case data source mapping is provided you can forrce to only do this for the
            sda hdi container
        """
        procedure_writer = HDBProcedureWriter(self.config)
        cds_writer = HDBCDSWriter(self.config)
        sql_key_sql = SqlProcessor.TRACE_KEY_SQL_PROCESSED
        if base_layer:
             # Base SDA layer procedures
            self.hana_helper._build_base_layer_artifacts(self.config.get_entry(ConfigConstants.CONFIG_KEY_PROCEDURES_PATH), data_source_mapping=sda_data_source_mapping_only)
        
        if consumption_layer:
            sql_processed_cons_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[SqlProcessor.TRACE_KEY_CONSUMPTION_LAYER]
            # Consumption layer procedures
            # We create for the output tables cds tables which we generate here
            for element in sql_processed_cons_layer:
                if not isinstance(element, dict):
                    continue # ignore TODO: proper doc
                
                if sql_key_sql in element: # TODO: gen warning if no sql
                    input = []
                    output = []
                    body = []

                    proc_name = element['name']

                    if 'input' in element[sql_key_sql]:
                        input = element[sql_key_sql]['input']
                    if 'body' in element[sql_key_sql]:
                        body = element[sql_key_sql]['body']
                    if 'output' in element[sql_key_sql]:
                        output = element[sql_key_sql]['output']
                    
                    # Build SQL array
                    sql = []
                    for item in input:
                        sql_str = ''
                        if 'sql_vars_syn' in item and item['sql_vars_syn']:
                            sql_str = item[sql_key_sql].format(*item['sql_vars_syn'])
                        else:
                            sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)
                    for item in body:
                        sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)
                    for item in output:
                        if 'sql_vars' in item:
                            sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)
                        
                    
                     # Explicitly disabling inputs on consumption layer as these are stand alone objects
                    signature_str = self.hana_helper._build_procedure_signature(None, output)
                    sql_str = StringUtils.flatten_string_array(sql)
                    if not sda_data_source_mapping_only:
                        sql_str = self.config.data_source_mapping(sql_str)
                    procedure_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_PROCEDURES_PATH), proc_name, sql_str, signature_str)

        # --CDS Generation
        # We always create CDS views as these are common components that can be used by solution specific implementation of the consumption layer
        sql_processed_cons_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[SqlProcessor.TRACE_KEY_CONSUMPTION_LAYER]
        cds_entries = []
        for element in sql_processed_cons_layer:
            if not isinstance(element, dict):
                continue # ignore TODO: proper doc
            if sql_key_sql in element: # TODO: gen warning if no sql
                output = []
                if 'output' in element[sql_key_sql]:
                        output = element[sql_key_sql]['output']
                for item in output: 
                    if 'object_name' in item and 'cds_type' in item:   
                        # Sanity check for not duplicating cds views
                        if not any(item['object_name'] in cds_entry for cds_entry in cds_entries):
                            cds_entries.append(self.hana_helper._build_cds_entity_entry(item['object_name'], item['cds_type']))
        # Generate hdbcds based on the generated cds entries
        cds_content = StringUtils.flatten_string_array(cds_entries) 
        cds_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_CDS_PATH), self.config.get_entry(ConfigConstants.CONFIG_KEY_CDS_CONTEXT), cds_content)


class HanaSDAGenerator(object):
    """
    This class provides HANA specific generation functionality for the Smart Data Access (SDA) 
    scenario. It only creates the artifact for the second SDA HDI container which loads and 
    uses data out of the first container which has been created before this class os called. 
    It also extend the config to cater for specific required config.
    """
    def __init__(self, config):
        """
        This is main entry point for generating the HANA related artifacts for the SDA scenario

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.hana_helper = HanaGeneratorHelper(config)
        self.config = config
        self._extend_config()

    def generate_artifacts(self, model_only=True):
        """
        Generate the artifacts by first building up the required folder structure for artifact storage and then 
        generating the different required files. Be aware that this method only generates the generic files
        and offloads the generation of artifacts where traversal of base and consumption layer 
        elements is required. 

        Parameters
        ----------
        model_only: boolean
            In the sda case we are only interested in transferring the model using SDA.
            This forces the HANA artifact generation to cater only for this scenario.

        Returns
        -------
        output_path : str
            Return the output path of the root folder where the related artifacts are stored.
        """
        self.hana_helper._build_folder_structure(ConfigConstants.PROJECT_TEMPLATE_BASE_SDA_STRUCT, self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_HANA ), self.config.get_entry( ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_MODULE))

        # Instantiate file writers
        yaml_writer = MTAYamlWriter(self.config)
        grant_writer = HDBGrantWriter(self.config)
        synonym_writer = HDBSynonymWriter(self.config)
        role_writer = HDBRoleWriter(self.config)
        consumption_processor = HanaSDAConsumptionProcessor(self.config)
        
        # Generate mta yaml file
        output_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_HANA)
        app_id = self.config.get_entry( ConfigConstants.CONFIG_KEY_SDA_APPID )
        module_name = self.config.get_entry( ConfigConstants.CONFIG_KEY_SDA_MODULE_NAME )
        version = self.config.get_entry( ConfigConstants.CONFIG_KEY_VERSION )
        schema = self.config.get_entry( ConfigConstants.CONFIG_KEY_SDA_SCHEMA )
        grant_service =self.config.get_entry( ConfigConstants.CONFIG_KEY_SDA_GRANT_SERVICE )
        yaml_writer.generate(output_path, app_id, module_name, version, schema, grant_service)

        # Generate hdbgrants
        remote_source = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_REMOTE_SOURCE)
        grant_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_GRANTS_PATH), remote_access=True, remote_source=remote_source)

        # Generate dataset/function synonym
        synonym_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_SYNONYMS_PATH)) 

        # Generate hdbroles for external access
        role_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_ROLES_PATH), name=self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_MODULE_NAME)) 

        # Generate Consumption Artifacts
        consumption_processor.generate(model_only)
        return output_path


    def _extend_config(self):
        """
        Extend the config to cater for HANA SDA generation specific config.
        """
        project_name = self.config.get_entry( ConfigConstants.CONFIG_KEY_PROJECT_NAME )
        sda_module_name =  project_name + '_sda'
        sda_app_id = sda_module_name
        sda_schema = '"'+(sda_module_name + '_SCHEMA').upper()+'"'
        
        sda_output_path_hana = os.path.join(self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH ), ConfigConstants.SDA_HANA_BASE_PATH )
        sda_output_path_module = os.path.join(sda_output_path_hana, sda_module_name)
        sda_output_path_module_src = os.path.join(sda_output_path_module, ConfigConstants.MODULE_SOURCE_PATH)
        sda_grants_path = os.path.join(sda_output_path_module_src, ConfigConstants.GRANTS_SOURCE_PATH)
        sda_synonyms_path = os.path.join(sda_output_path_module_src, ConfigConstants.SYNONYMS_SOURCE_PATH)
        sda_procedures_path = os.path.join(sda_output_path_module_src, ConfigConstants.PROCEDURES_SOURCE_PATH)
        sda_roles_path = os.path.join(sda_output_path_module_src, ConfigConstants.ROLES_SOURCE_PATH)
        sda_virtual_table_path = os.path.join(sda_output_path_module_src, ConfigConstants.VIRTUAL_TABLE_SOURCE_PATH)
        sda_cds_path = os.path.join(sda_output_path_module_src, ConfigConstants.CDS_SOURCE_PATH)

        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_MODULE_NAME, sda_module_name)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_APPID, sda_app_id)

        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_HANA, sda_output_path_hana)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_MODULE, sda_output_path_module)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_MODULE_SRC, sda_output_path_module_src)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_GRANTS_PATH, sda_grants_path) 
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_SYNONYMS_PATH, sda_synonyms_path)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_PROCEDURES_PATH, sda_procedures_path)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_ROLES_PATH, sda_roles_path)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_VIRTUALTABLE_PATH, sda_virtual_table_path)
        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_CDS_PATH, sda_cds_path)

        self.config.add_entry( ConfigConstants.CONFIG_KEY_SDA_SCHEMA, sda_schema)


class HanaSDAConsumptionProcessor(object):
    """
    This class provides HANA SDA specific generation functionality for the SDA HDI container. 
    It utilizes the consumption layer as reference to generate the respective required 
    artifacts.
    """
    def __init__(self, config):
        """
        This class allows to generate the arifacts for the SDA HDI container. 

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.hana_helper = HanaGeneratorHelper(config)
        self.config = config

    def generate(self, model_only=True):
        """
        Method for generating the actual artifacts content.  

        Parameters
        ----------
        model_only: boolean
            In the sda case we are only interested in transferring the model using SDA.
            This forces the HANA artifact generation to cater only for this scenario.
        """
        cds_writer = HDBCDSWriter(self.config)
        procedure_writer = HDBProcedureWriter(self.config)
        sql_key_sql = SqlProcessor.TRACE_KEY_SQL_PROCESSED
        procedure_gen_filter = None
        if model_only:
            procedure_gen_filter = ['predict', 'partition']

        # Base SDA layer procedures
        self.hana_helper._build_base_layer_artifacts(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_PROCEDURES_PATH), procedure_gen_filter, data_source_mapping=True) #Always do datasource mapping

        # Consumption layer procedures
        sql_processed_cons_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[SqlProcessor.TRACE_KEY_CONSUMPTION_LAYER]
        # We create for the output tables cds tables which we generate here
        cds_sda_entries = []
        for element in sql_processed_cons_layer:
            if not isinstance(element, dict):
                continue # ignore TODO: proper doc
            
            if sql_key_sql in element: # TODO: gen warning if no sql
                proc_name = element['name']
                include_procedure = True

                # We only want the partition and predict procedures. For now others are taken out for the SDA
                # However the fit result we do want. So we only set a flag and process as normal, but we will
                # not generate the procedure artefact for this consumption layer element.
                if procedure_gen_filter:
                   if not any(gen_filter in proc_name for gen_filter in procedure_gen_filter):
                       include_procedure = False

                input = []
                output = []
                body = []

                if 'input' in element[sql_key_sql]:
                    input = element[sql_key_sql]['input']
                if 'body' in element[sql_key_sql]:
                    body = element[sql_key_sql]['body']
                if 'output' in element[sql_key_sql]:
                    output = element[sql_key_sql]['output']
                
                # Build SQL array (slight overhead due to inlude_proc flag. if false then no need to do the rest)
                # TODO refactor 
                sql = []
                for item in input:
                    sql_str = item[sql_key_sql].format(*item['sql_vars'])
                    sql.append(sql_str)
                for item in body:
                    sql_str = item[sql_key_sql].format(*item['sql_vars'])
                    sql.append(sql_str)
                for item in output:
                    # Only generate cds entity if the procedure is included
                    if include_procedure:
                        sql_str = ''
                        if 'sql_vars_syn' in item and item['sql_vars_syn']:
                            sql_str = item[sql_key_sql].format(*item['sql_vars_syn'])
                        else:
                            sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)
                        cds_sda_entries.append(self._generate_sda_cds(item, extend=False))

                    if model_only:
                        # Only generate SDA in case of MODEL output otherwise continue
                        # if not item['cat'] == 'MODEL':
                        if not self.config.is_model_category(item['cat']):
                            continue
                    # We force the model to also be created as antity as we need it for the load proc
                    cds_sda_entries.append(self._generate_sda_cds(item, extend=True))
                    self._generate_sda(proc_name, item)
                    
                
                # Check if we need to filter out any procedure creation
                if not include_procedure:
                    continue

                # Explicitly disabling inputs on consumption layer as these are stand alone objects
                signature_str = self.hana_helper._build_procedure_signature(None, output)
                sql_str = StringUtils.flatten_string_array(sql)
                sql_str = self.config.data_source_mapping(sql_str)
                procedure_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_PROCEDURES_PATH), proc_name, sql_str, signature_str)
                    
        cds_sda_content = StringUtils.flatten_string_array(cds_sda_entries)
        cds_writer.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_CDS_PATH), self.config.get_entry(ConfigConstants.CONFIG_KEY_CDS_CONTEXT), cds_sda_content)

    def _generate_sda(self, proc_name, item):
        """
        Method for generating the sda specific content for a consumption layer element.  

        Parameters
        ----------
        proc_name: str
            In the sda case we are only interested in transferring the model using SDA.
            This forces the HANA artifact generation to cater only for this scenario.
        item : dict
            Consumption layer element.
        """
        # Writers
        procedure_writer = HDBProcedureWriter(self.config)
        virtual_table_writer = HDBVirtualTableWriter(self.config)
        load_proc_name = 'load_' + proc_name + '_' + item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_INT_NAME]
        source_table = item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_DBOBJECT_NAME]
        source_schema = item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_SCHEMA]
        virtual_table = 'remote_' + item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_INT_NAME]
        target_table = item[SqlProcessor.TRACE_KEY_TABLES_ATTRIB_DBOBJECT_NAME] # name in the SDA container is same as source container
        virtual_table_output_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_VIRTUALTABLE_PATH)
        sda_load_proc_output_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_PROCEDURES_PATH)
        remote_source = self.config.get_entry( ConfigConstants.CONFIG_KEY_SDA_REMOTE_SOURCE )
        virtual_table_writer.generate(virtual_table_output_path, remote_source, source_schema, source_table, virtual_table)
        load_sql = self._build_sda_load_sql(virtual_table, target_table, self._get_sda_cds_extension_values())
        procedure_writer.generate(sda_load_proc_output_path, load_proc_name, load_sql, '')
    
    def _generate_sda_cds(self, item, extend=False):
        """
        Method for generating the sda specific cds content for a consumption layer element.  

        Parameters
        ----------
        item : dict
            Consumption layer element.
        extend : boolean
            Add additional fields which are specifically required for SDA

        Returns
        -------
        cds_entity_item : dict
            The build cds entity item
        """
        cds_type_extension_values = self._get_sda_cds_extension_values()
        cds_type_extension = None
        if extend:
            cds_type_extension = self._build_sda_cds_type_extension(cds_type_extension_values)
        return self.hana_helper._build_cds_entity_entry(item['object_name'], item['cds_type'], cds_type_extension)

    def _get_sda_cds_extension_values(self):
        """
        Currently the additional required fields required for SDA generation are defined here. 
        For it is not dynamically setup. But this can be catered for by extending this methd.
            
        Returns
        -------
        cds_type_extension_values : dict
            The build cds entity type additional SDA fields
        """
        cds_type_extension_values = [
            {
                'column': 'VERSION', 
                'data_type': 'Integer',
                'sql': '1'
            }, 
            {
                'column': 'LOADED_ON', 
                'data_type': 'UTCTimestamp',
                'sql': 'CURRENT_UTCTIMESTAMP'
            }
        ] # TODO impove hardcoded version to support model versioning
        return cds_type_extension_values

    def _build_sda_cds_type_extension(self, elements):
        """
        Build the actual cds type extension
            
        Returns
        -------
        extension_str : str
            The extension string that needs to be appended to the cds entity type
        """
        indent = '      '
        extension_str = ''
        for element in elements:
            column = element['column']
            data_type = element['data_type']
            extension_str += indent + column + ' : ' + data_type + ';\n'
        return extension_str

    def _build_sda_load_sql(self, virtual_table, target_table, cds_type_extension_values=None):
        """
        Build the sda load sql for loading data from the first HDI container.
            
        Returns
        -------
        sql : str
            The sql that needs to be written as part of the procedure file
        """
        sql = 'TRUNCATE TABLE {}; \n'
        sql += 'INSERT INTO {} SELECT *'
        for extension in cds_type_extension_values:
            sql += ',' + extension['sql']
        sql += ' FROM "::{}";\n'
        sql += 'SELECT * FROM {};'
        return sql.format(target_table, target_table, virtual_table, target_table)


class HanaGeneratorHelper(object):
    """
    This class provides generic helper function for HANA generation functionality.  
    It generates the files for the base layer elements.
    """
    def __init__(self, config):
        """
        This class provides helper methods when generating arifacts plus provides the
        generation of base layer artifacts which are HANA only. 

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.directory_handler = DirectoryHandler()
        self.config = config
    
    def _build_base_layer_artifacts(self, path, gen_filters=None, data_source_mapping=False):
        """
        Build the sda load sql for loading data from the first HDI container.
        
        Parameters
        ----------
        path : str
            output path of the base layer artifacts
        gen_filters : list
            string list of procedures to include
        data_source_mapping: boolean
            Whether to apply data source mapping

        Returns
        -------
        sql : str
            The sql that needs to be written as part of the procedure file
        """
        procedure_writer = HDBProcedureWriter(self.config)
        sql_key_input = SqlProcessor.TRACE_KEY_TABLES_INPUT_PROCESSED
        sql_key_tables_output = SqlProcessor.TRACE_KEY_TABLES_OUTPUT_PROCESSED
        sql_key_vars_output = SqlProcessor.TRACE_KEY_VARS_OUTPUT_PROCESSED
        sql_key_sql = SqlProcessor.TRACE_KEY_SQL_PROCESSED
        sql_processed_base_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[SqlProcessor.TRACE_KEY_BASE_LAYER]
        for algo in sql_processed_base_layer:
            if not isinstance(sql_processed_base_layer[algo], dict):
                continue
            for function in sql_processed_base_layer[algo]:
                if sql_key_sql in sql_processed_base_layer[algo][function]:
                    proc_name = sql_processed_base_layer[algo][function][SqlProcessor.TRACE_KEY_METADATA_PROCESSED][SqlProcessor.TRACE_KEY_METADATA_ATTRIB_PROC_NAME]
                    if gen_filters:
                        if not any(gen_filter in proc_name for gen_filter in gen_filters):
                            continue
                    input = []
                    output = []
                    if sql_key_input in sql_processed_base_layer[algo][function]:
                        input = sql_processed_base_layer[algo][function][sql_key_input]
                    if sql_key_tables_output in sql_processed_base_layer[algo][function]:
                        output = sql_processed_base_layer[algo][function][sql_key_tables_output]
                    if sql_key_vars_output in sql_processed_base_layer[algo][function]:
                        output.extend(sql_processed_base_layer[algo][function][sql_key_vars_output])
                    
                    signature_str = self._build_procedure_signature(input, output)
                    sql_str = StringUtils.flatten_string_array(sql_processed_base_layer[algo][function][sql_key_sql])
                    if data_source_mapping:
                        sql_str = self.config.data_source_mapping(sql_str)
                    procedure_writer.generate(path, proc_name, sql_str, signature_str)


    def _build_cds_entity_entry(self, entity_name, cds_type, cds_type_extension=None):
        """
        Build up the cds entity entry string which can be used as element in the hdbdd file
        
        Parameters
        ----------
        entity_name : str
            Name of the entity 
        cds_type : str
            Type to use for the entity
        cds_type_extension: str
            Whether to add type extension fields.

        Returns
        -------
        cds_entry : str
            The cds entry string
        """
        if cds_type:
            indent = '  '
            cds_entry = indent + 'entity ' +  entity_name + ' {\n'
            cds_entry += cds_type
            if cds_type_extension:
                cds_entry += cds_type_extension
            cds_entry += indent + '};\n'
            return cds_entry
        return None

    def _build_folder_structure(self, base_structure, output_path, module_output_path):
        """
        Build up the folder structure based on a template folder structure provided as part
        of the artifact package. The templates are stored in:
        /hana_ml_artifact/generators/<module_template_path>/<project_template_base_dir>/<base_structure>
        ie: /hana_ml_artifacts/generators/templates/hana/base_sda_structure
        
        Parameters
        ----------
        base_structure : str
            Physical location of the template base folder structure
        output_path : str
            Physical root target location which will be cleaned
        module_output_path: str
            Physical location of where the module (HDI container) needs to be populated with the
            respective required artifacts. 
        """
        self._clean_folder_structure(output_path)
        
        # Parse and copy template base project structure
        module_template_path = self.config.get_entry(ConfigConstants.CONFIG_KEY_MODULE_TEMPLATE_PATH)
        base_structure_template_path = os.path.join(module_template_path, ConfigConstants.PROJECT_TEMPLATE_BASE_DIR, base_structure)
        self.directory_handler.copy_directory(base_structure_template_path, module_output_path)

    def _clean_folder_structure(self, path):
        """
        Clean up physical folder structure. 
        
        Parameters
        ----------
        path : str
            Physical location to clean
        """
        if os.path.exists(path):
            self.directory_handler.delete_directory_content(path)
            os.rmdir(path)

    
    def _build_procedure_signature(self, input_tables, output_tables):
        """
        Based on input and output tables generate the procedure signature
        
        Parameters
        ----------
        input_tables : list
            list of input tables
        output_tables : list
            list of output tables
        
        Returns
        -------
        signature_str: str
            the signature string which can be used in the procedure (hdbprocedure)
        """
        signature_str = ''

        # Input
        if input_tables:
            signature_str += self._build_procedure_interface(input_tables, 'in')
            if output_tables:
                signature_str += ', '
        
        # Output
        if output_tables:
            signature_str += self._build_procedure_interface(output_tables, 'out')

        return signature_str

    def _build_procedure_interface(self, items, str_type):
        """
        Build input or output signature part
        
        Parameters
        ----------
        items : list
            list of items to generate as part of this signature part
        str_type : str
            whether it is the in or the out part of the procedure signature
        
        Returns
        -------
        interface_str: str
            the signature part of these items
        """
        sql_key_ttype = SqlProcessor.TRACE_KEY_TABLES_ATTRIB_TYPE
        sql_key_vtype = SqlProcessor.TRACE_KEY_VARS_ATTRIB_DATA_TYPE
        sql_key_name = SqlProcessor.TRACE_KEY_TABLES_ATTRIB_INT_NAME
        interface_str = ''
        for idx, item in enumerate(items):
            if idx > 0:
                interface_str += ', '
            interface_str += str_type
            if sql_key_ttype in item:
                interface_str += ' ' + item[sql_key_name] + ' ' + item[sql_key_ttype]
            if sql_key_vtype in item:
                interface_str += ' ' + item[sql_key_name] + ' ' + item[sql_key_vtype]
        return interface_str