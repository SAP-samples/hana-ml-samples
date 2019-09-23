"""
This module handles the generation of the files that represent the
artifacts.
"""
import os
import json

from .filewriter_base import FileWriterBase

from ...sql_processor import SqlProcessor
from ...hana_ml_utils import StringUtils
from ...config import ConfigConstants


class MTAYamlWriter(FileWriterBase):
    """
    This class generates a yaml file
    """

    def generate(self, path, app_id, module_name, version, schema, grant_service):
        """
        Generate the yaml based on a template

        Parameters
        ----------
        path: str
            Physical location where to write the file
        app_id: str
            The application id of the HDI container
        module_name : str
            The module name of the HDI container
        version : str
            The version of the HDI container
        schema : str
            The schema to be used by the HDI container
        grant_service : str
            The grant service to be used by the HDI container
        """
        self.write_file(path, app_id, module_name, version, schema, grant_service)

    def write_file(self, path, app_id, module_name, version, schema, grant_service):
        """
        Retrieve the template yaml file and generate the replacements for the template
        before writing the file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        app_id: str
            The application id of the HDI container
        module_name : str
            The module name of the HDI container
        version : str
            The version of the HDI container
        schema : str
            The schema to be used by the HDI container
        grant_service : str
            The grant service to be used by the HDI container
        """
        template_file_path = os.path.join(
            os.path.dirname(__file__),
            ConfigConstants.TEMPLATE_DIR, ConfigConstants.YAML_TEMPLATE_FILE)
        replacements = {
            ConfigConstants.YAML_TEMPLATE_APPID_PLACEHOLDER: app_id,
            ConfigConstants.YAML_TEMPLATE_NAME_PLACEHOLDER: module_name,
            ConfigConstants.YAML_TEMPLATE_VERSION_PLACEHOLDER: version,
            ConfigConstants.YAML_TEMPLATE_SCHEMA_PLACEHOLDER: schema,
            ConfigConstants.YAML_TEMPLATE_UPS_GRANT_PLACEHOLDER: grant_service
        }
        self.write_template(path, ConfigConstants.YAML_FILE_NAME, template_file_path, replacements)


class HDBGrantWriter(FileWriterBase):
    """
    This class generates a grant file
    """

    def generate(self, path, remote_access=False, remote_source=''):
        """
        Generate the yaml based on a template

        Parameters
        ----------
        path: str
            Physical location where to write the file
        remote_access: boolean
            In case we are dealing with the SDA scenario
        remote_source : str
            The remote source to be used in a SDA scenario
        """
        template_file_path = os.path.join(
            os.path.dirname(__file__),
            ConfigConstants.TEMPLATE_DIR, ConfigConstants.GRANT_TMP_FILE_NAME)
        replacements = {}

        if remote_access:
            template_file_path = os.path.join(
                os.path.dirname(__file__),
                ConfigConstants.TEMPLATE_DIR, ConfigConstants.GRANT_TMP_SDA_FILE_NAME)
            replacements[ConfigConstants.GRANT_TEMPLATE_SDA_REMOTE_SOURCE] = remote_source

        sql_processed = self.get_config_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[
            SqlProcessor.TRACE_KEY_BASE_LAYER]

        # AFL execute grant is part of the template as this is always required. However dataset is dynamic. So we use the
        # synonyms to generate the dataset privileges. Only SELECT is provided. The grants are also added to the SDA container
        schema_privileges = []
        schema_privileges_with = []
        for algo in sql_processed:
            # Exclude relations context from processing
            if not algo is SqlProcessor.TRACE_KEY_RELATION_CONTEXT:
                if not isinstance(sql_processed[algo], dict):
                    continue  # ignore TODO: proper doc
                for function in sql_processed[algo]:
                    if SqlProcessor.TRACE_KEY_SYNONYMS_PROCESSED in sql_processed[algo][function]:
                        for synonym in sql_processed[algo][function][
                                SqlProcessor.TRACE_KEY_SYNONYMS_PROCESSED]:
                            if SqlProcessor.SYNONYM_STRUCT_KEY_TYPE in synonym and synonym[SqlProcessor.SYNONYM_STRUCT_KEY_TYPE] == SqlProcessor.TRACE_KEY_DATASET:
                                schema_privilege_with = {
                                    'reference': synonym['schema'],
                                    'privileges_with_grant_option': ['SELECT']
                                }
                                schema_privileges_with.append(schema_privilege_with)
                                schema_privilege = {
                                    'reference': synonym['schema'],
                                    'privileges': ['SELECT']
                                }
                                schema_privileges.append(schema_privilege)

        schema_privileges_str = json.dumps(schema_privileges)
        schema_privileges_with_str = json.dumps(schema_privileges_with)

        replacements[ConfigConstants.GRANT_TEMPLATE_SCHEMA_PRIVILEGES] = schema_privileges_str
        replacements[ConfigConstants.GRANT_TEMPLATE_SCHEMA_PRIVILEGES_WITH] = schema_privileges_with_str

        self.write_file(path, template_file_path, replacements)

    def write_file(self, path, template_file_path, replacements):
        """
        Write the file based on a template

        Parameters
        ----------
        path: str
            Physical location where to write the file
        template_file : str
            Location of the template file
        replacements : dict
            Replacements for the template placeholders
        """
        file_name = ConfigConstants.GRANT_FILE_NAME + ConfigConstants.GRANT_FILE_EXTENSION
        self.write_template(path, file_name, template_file_path, replacements)


class HDBProcedureWriter(FileWriterBase):
    """
    This class generates a procedure file
    """

    def generate(self, path, procedure_name, sql_str, signature_str):
        """
        Generate the file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        procedure_name : str
            Procedure name
        sql_str : str
            SQL content of the procedure
        signature_str : str
            Signatre of the procedure
        """
        self.write_file(path, procedure_name, sql_str, signature_str)

    def write_file(self, path, procedure_name, sql_str, signature_str):
        """
        Retrieve the template procedure file and generate the replacements for the template
        before writing the file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        procedure_name : str
            Procedure name
        sql_str : str
            SQL content of the procedure
        signature_str : str
            Signatre of the procedure
        """
        template_file_path = os.path.join(
            os.path.dirname(__file__),
            ConfigConstants.TEMPLATE_DIR, ConfigConstants.PROCEDURE_TEMPLATE_FILE)
        replacements = {
            ConfigConstants.PROCEDURE_TEMPLATE_SQL_PLACEHOLDER: sql_str,
            ConfigConstants.PROCEDURE_TEMPLATE_PROC_NAME: procedure_name,
            ConfigConstants.PROCEDURE_TEMPLATE_PROC_INTERFACE: signature_str,
        }
        file_name = procedure_name+ConfigConstants.PROCEDURE_FILE_EXTENSION

        self.write_template(path, file_name, template_file_path, replacements)


class HDBSynonymWriter(FileWriterBase):
    """
    This class generates a synonym file
    """

    def generate(self, path):
        """
        Generating the content before writing the file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        """
        sql_processed = self.get_config_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[
            SqlProcessor.TRACE_KEY_BASE_LAYER]
        target_ojects = []
        # For now all synonyms are merged into one synonym file
        for algo in sql_processed:
            # Exclude relations context from processing
            if not algo is SqlProcessor.TRACE_KEY_RELATION_CONTEXT:
                if not isinstance(sql_processed[algo], dict):
                    continue  # ignore TODO: proper doc
                for function in sql_processed[algo]:
                    if SqlProcessor.TRACE_KEY_SYNONYMS_PROCESSED in sql_processed[algo][function]:
                        target_ojects.extend(
                            sql_processed[algo][function]
                            [SqlProcessor.TRACE_KEY_SYNONYMS_PROCESSED])

        self.write_file(path, target_ojects)

    def write_file(self, path, target_objects):
        """
        Map the target objects to the content of the file before writing the file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        target_objects : list
            Procedure name
        """
        gen_content = {}
        target_mapping = {}
        for target_object in target_objects:
            if target_object["schema"] and target_object["object"]:
                gen_content[target_object['synonym']] = {
                    "target": {
                        "object": target_object["object"],
                        "schema": target_object["schema"]
                    }
                }
                target_mapping[target_object["schema"] + '."' +
                               target_object["object"] + '"'] = '"' + target_object['synonym'] + '"'

        file_name = ConfigConstants.SYNONYM_FILE_NAME + ConfigConstants.SYNONYM_FILE_EXTENSION
        self.write_content(path, file_name, json.dumps(gen_content))


class HDBRoleWriter(FileWriterBase):
    """
    This class generates a role file
    """

    def generate(self, path, name=None):
        """
        Generating the role file

        Parameters
        ----------
        path : str
            Physical location where to write the file
        name : str
            In case an alternative name for the role is required
        """
        if not name:
            name = self.get_config_entry('project_name')
        self.write_file(path, name)

    def write_file(self, path, name):
        """
        Retrieve the template role file and generate the replacements for the template
        before writing the file. We create a role file for standard access and with grant 
        rights.

        Parameters
        ----------
        path : str
            Physical location where to write the file
        name : str
            In case an alternative name for the role is required
        """
        template_file_path = os.path.join(
            os.path.dirname(__file__),
            ConfigConstants.TEMPLATE_DIR, ConfigConstants.ROLE_TEMPLATE_FILENAME)
        template_file_with_path = os.path.join(
            os.path.dirname(__file__),
            ConfigConstants.TEMPLATE_DIR, ConfigConstants.ROLE_TEMPLATE_FILENAME_WITH)
        role_name = 'hanaml::' + name.replace('_', '')
        file_name = 'role' + ConfigConstants.ROLE_FILE_EXTENSION
        file_with_name = 'role_with' + ConfigConstants.ROLE_FILE_EXTENSION
        replacements = {
            ConfigConstants.ROLE_TEMPLATE_ROLE_NAME_PLACEHOLDER: role_name
        }
        self.write_template(path, file_name, template_file_path, replacements)
        self.write_template(path, file_with_name, template_file_with_path, replacements)


class HDBVirtualTableWriter(FileWriterBase):
    """
    This class generates a virtual table file
    """

    def generate(self, path, remote_source, source_schema, source_table, virtual_table):
        """
        Generating the virtual table file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        remote_source : str
            The remote source to be used in a SDA scenario
        source_schema : str
            The source schema the virtual table is targeting
        source_table : str
            The source table the virtual table is targeting
        virtual_table : str
            The virtual table name to be used
        """
        self.write_file(path, remote_source, source_schema, source_table, virtual_table)

    def write_file(self, path, remote_source, source_schema, source_table, virtual_table):
        """
        Retrieve the template virtual table file and generate the replacements for the template
        before writing the file. 

        Parameters
        ----------
        path: str
            Physical location where to write the file
        remote_source : str
            The remote source to be used in a SDA scenario
        source_schema : str
            The source schema the virtual table is targeting
        source_table : str
            The source table the virtual table is targeting
        virtual_table : str
            The virtual table name to be used
        """
        template_file_path = os.path.join(
            os.path.dirname(__file__),
            ConfigConstants.TEMPLATE_DIR, ConfigConstants.VIRTUAL_TABLE_TEMPLATE_FILENAME)
        extended_virtual_table = '::'+virtual_table
        source_schema_table = source_schema + '.' + source_table
        file_name = virtual_table + ConfigConstants.VIRTUAL_TABLE_FILE_EXTENSION
        replacements = {
            ConfigConstants.VIRTUAL_TABLE_TEMPLATE_CONNECTION_NAME_PLACEHOLDER: remote_source,
            ConfigConstants.VIRTUAL_TABLE_TEMPLATE_SOURCE_SCHEMA_TABLE_PLACEHOLDER: source_schema_table,
            ConfigConstants.VIRTUAL_TABLE_TEMPLATE_VIRTUAL_TABLE_PLACEHOLDER: extended_virtual_table
        }
        self.write_template(path, file_name, template_file_path, replacements)


class HDBCDSWriter(FileWriterBase):
    """
    This class generates a cds file
    """

    def generate(self, path, context_name, context_content):
        """
        Generating the cds file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        context_name : str
            The cds context name
        context_content : str
            The cds context content
        """
        self.write_file(path, context_name, context_content)

    def write_file(self, path, context_name, context_content):
        """
        Retrieve the template cds file and generate the replacements for the template
        before writing the file. 

        Parameters
        ----------
        path: str
            Physical location where to write the file
        context_name : str
            The cds context name
        context_content : str
            The cds context content
        """
        template_file_path = os.path.join(
            os.path.dirname(__file__),
            ConfigConstants.TEMPLATE_DIR, ConfigConstants.CDS_TEMPLATE_FILENAME)
        file_name = context_name + ConfigConstants.CDS_FILE_EXTENSION
        replacements = {
            ConfigConstants.CDS_TEMPLATE_CONTEXT_NAME: context_name,
            ConfigConstants.CDS_TEMPLATE_CONTEXT_CONTENT: context_content
        }
        self.write_template(path, file_name, template_file_path, replacements)
