"""
This module handles the processing of the sql trace into base layer and if required
consumption layer objects. It also implements the merging and grouping of elements
in the sql trace.
"""
import copy
import logging
import uuid

import hana_ml as hanaml  # Only here for version validation.

from .config import ConfigConstants
from .hana_ml_utils import StringUtils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class SqlProcessorBase(object):
    """
    This is the base class which holds constants which are specific to the SQL processor.
    Furthermore it holds generic helper methods.
    """
    TRACE_KEY_BASE_LAYER = 'base_layer'
    TRACE_KEY_CONSUMPTION_LAYER = 'consumption_layer'
    TRACE_KEY_RELATION_CONTEXT = 'relations'
    TRACE_KEY_SQL = 'sql'
    TRACE_KEY_SQL_PROCESSED = 'sql'
    TRACE_KEY_ALGO = 'algo'
    TRACE_KEY_FUNCTION = 'function'
    TRACE_KEY_DATASET = 'dataset'
    TRACE_KEY_NAME = 'name'
    TRACE_KEY_SCHEMA = 'schema'
    TRACE_KEY_TABLES_INPUT = 'input_tables'
    TRACE_KEY_TABLES_INPUT_PROCESSED = TRACE_KEY_TABLES_INPUT
    TRACE_KEY_TABLES_INTERNAL = 'internal_tables'
    TRACE_KEY_TABLES_INTERNAL_PROCESSED = TRACE_KEY_TABLES_INTERNAL
    TRACE_KEY_TABLES_OUTPUT = 'output_tables'
    TRACE_KEY_TABLES_OUTPUT_PROCESSED = TRACE_KEY_TABLES_OUTPUT
    TRACE_KEY_VARS_OUTPUT = 'output_vars'
    TRACE_KEY_VARS_OUTPUT_PROCESSED = TRACE_KEY_VARS_OUTPUT
    # Name as provided by hana ml api
    TRACE_KEY_ATTRIB_NAME = 'name'
    # Select statement as provided by hana ml api
    TRACE_KEY_ATTRIB_SELECT = 'select'
    # Name as provided by hana ml api
    TRACE_KEY_TABLES_ATTRIB_NAME = TRACE_KEY_ATTRIB_NAME
    # Bare transformed table name for easy reference
    TRACE_KEY_TABLES_ATTRIB_TRNAME = 'transformed_name'
    # Whether the table has been initiatied in the sql already
    TRACE_KEY_TABLES_ATTRIB_IN_SQL = 'in_sql'
    # Select statement as provided by hana ml api
    TRACE_KEY_TABLES_ATTRIB_SELECT = TRACE_KEY_ATTRIB_SELECT
    # Signature as provided by hana ml api
    TRACE_KEY_TABLES_ATTRIB_TYPE = 'table_type'
    # Table categorie
    TRACE_KEY_TABLES_ATTRIB_CAT = 'cat'
    # User friendly name to use as part of a signature.
    # Mapping will be done by adding sql statemetns
    TRACE_KEY_TABLES_ATTRIB_INT_NAME = 'interface_name'
    # Proposed dbobject name in case table needs to be persisted as physical table.
    TRACE_KEY_TABLES_ATTRIB_DBOBJECT_NAME = 'dbobject_name'
    TRACE_KEY_TABLES_ATTRIB_SCHEMA = 'dbobject_name_schema'
    TRACE_KEY_TABLES_ATTRIB_OBJECT_NAME = 'object_name'
    TRACE_KEY_TABLES_ATTRIB_SYNONYM = 'synonym'
    # Name as provided by hana ml api
    TRACE_KEY_VARS_ATTRIB_NAME = TRACE_KEY_ATTRIB_NAME
    # Bare transformed table name for easy reference
    TRACE_KEY_VARS_ATTRIB_TRNAME = 'transformed_name'
    # Select statement as provided by hana ml api
    TRACE_KEY_VARS_ATTRIB_SELECT = TRACE_KEY_ATTRIB_SELECT
    TRACE_KEY_VARS_ATTRIB_TYPE = 'type'
    # Signature as provided by hana ml api
    TRACE_KEY_VARS_ATTRIB_DATA_TYPE = 'data_type'
    # Table categorie
    TRACE_KEY_VARS_ATTRIB_CAT = 'cat'
    # User friendly name to use as part of a signature.
    # Mapping will be done by adding sql statemetns
    TRACE_KEY_VARS_ATTRIB_INT_NAME = TRACE_KEY_TABLES_ATTRIB_INT_NAME
    TRACE_KEY_VARS_ATTRIB_OBJECT_NAME = TRACE_KEY_TABLES_ATTRIB_OBJECT_NAME
    TRACE_KEY_SYNONYMS_PROCESSED = 'synonyms'
    TRACE_KEY_METADATA_PROCESSED = 'metadata'
    TRACE_KEY_METADATA_ATTRIB_PROC_NAME = 'procedure_name'
    TRACE_KEY_METADATA_ATTRIB_ALGO_FUNCTION_CAT = 'algo_function_cat'
    TABLE_CATEGORIES = ['DATA', 'MODEL', 'PARAM', 'STATISTICS', 'FITTED', 'VAR_IMPORTANCE',
                        'OOB_ERR', 'CM', 'RULES', 'CM', 'STATS', 'CV', 'RESULT', 'METRIC']
    VAR_CATEGORIES = ['METRIC']
    ALFO_FUNCTION_CATEGORIES = ['PARTITION', 'FIT', 'PREDICT', 'FIT_PREDICT', 'SCORE']
    SYNONYM_STRUCT_KEY_OBJECT = 'object'
    SYNONYM_STRUCT_KEY_SCHEMA = 'schema'
    SYNONYM_STRUCT_KEY_SYNONYM = 'synonym'
    SYNONYM_STRUCT_KEY_TYPE = 'type'
    SUPPORTED_ALGOS = ['RANDOMFORESTCLASSIFIER']

    def __init__(self, config):
        """
        Sql processing base class for artifact generation.

        Parameters
        ----------
        config : dict
            The config object holds the different configuration options required for
            generation.
        """
        self.config = config

    def _parse_dataset_sql(self, dataset_sql): #pyling: disable=no-self-use
        """
        Parse the sql fof the dataset. This to distill the schema and table

        Parameters
        ----------
        dataset_sql : str
            dataset sql string

        Returns
        -------
        schema : str
            schema of the dataset
        table : str
            table of the dataset

        """
        dataset_sql = dataset_sql.upper()
        from_count = StringUtils.count_words(dataset_sql, 'FROM')
        #  First make sure we only get the inner select from to assure we get the dataset itself.
        dataset_sql = dataset_sql[StringUtils.findnth(dataset_sql, 'FROM', from_count):]

        # Just get the schema / object part of the sql
        if StringUtils.count_char(dataset_sql, ' ') > 1:
            dataset_sql = dataset_sql[StringUtils.findnth(
                dataset_sql, ' ', 1):StringUtils.findnth(dataset_sql, ' ', 2)]
        else:
            dataset_sql = dataset_sql[StringUtils.findnth(dataset_sql, ' ', 1):]

        # Clean up
        dataset_sql = dataset_sql.strip()
        dataset_sql = dataset_sql.replace(')', '')
        dataset_sql = dataset_sql.replace('"', '')

        dataset_split = dataset_sql.split('.')
        if len(dataset_split) == 2:
            return dataset_split[0], dataset_split[1]
        return None, dataset_split[0]

    def _get_attribute_from_path(self, sql_processed, path, attribute):
        """
        Get a certain attribute from the sql_processed dictionary structure.
        This allows for direct access to attributes deeper in the struture.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace
        path : str
            The path from which the attribute value needs to be taken.
        attribute: str
            The attribute from which we would like to take the value.

        Returns
        -------
        target_value : object
            return the value of the attribute requested

        """
        layer, algo, function = self._get_path_parts(path)
        target_object = sql_processed[layer][algo][function]
        if attribute in target_object:
            return target_object[attribute]

    def _clean_string(self, value, full=False):
        """
        Clean the string values.

        Parameters
        ----------
        value : str
            string to clean
        full : boolean
            Full is to remove all special charactes otherwise only bare minimum.
            cleaning (# and ") is done which is default.

        Returns
        -------
        cleaned_str : str
            cleaned string

        """
        if full:
            return StringUtils.remove_special_characters(value)
        if not full:
            replacements = {
                '"': '',
                '#': ''
            }
            return StringUtils.multi_replace(value, replacements)

    def _generate_path(self, parts):
        """
        Generate dictionary path.

        Parameters
        ----------
        parts : list
            list of parts of the dictionary path

        Returns
        -------
        path : str
            generated path

        """
        seperator = '/'
        return seperator.join(parts)

    def _get_path_parts(self, path):
        """
        Split a dictionary path into its parts.

        Parameters
        ----------
        path : str
            dictionary path

        Returns
        -------
        parts : list
            list of parts of the dictionary path

        """
        return path.split('/')

    def _get_raw_sql(self, sql_trace):
        """
        Split a dictionary path into its parts.

        Parameters
        ----------
        sql_trace : dictionary
            raw sql trace structure generated in the hana ml package

        Returns
        -------
        sqls : list
            list of all sql entries.

        """
        sqls = ''
        for algo in sql_trace:
            for function in sql_trace[algo]:
                sqls += '------START Algo {} and Function {} ------'.format(algo, function)
                for sql_entry in sql_trace[algo][function][self.TRACE_KEY_SQL]:
                    sqls += sql_entry.replace('\n', '') + ';' + '\n'
                sqls += '------END Algo {} and Function {} ------'.format(algo, function)
        return sqls

    def _generate_db_object_name(self, parts, hdbtable=False):
        """
        Generate a db object name generically

        Parameters
        ----------
        parts : list
            parts that will make up the name
        hdbtable : boolean
            generation for table or not.

        Returns
        -------
        object_name : str
            object name which can be used for internal reference
        db_object_name : str
            object name as to be used in the db and in related artifacts such as procedures

        """
        object_name = self._generate_object_name(parts)
        db_object_name = '"' + self.config.get_entry(ConfigConstants.CONFIG_KEY_CDS_CONTEXT) + \
                         '.' + object_name + '"'

        # Add schema and proper syntax
        if hdbtable:
            object_name = self.config.get_entry(ConfigConstants.CONFIG_KEY_SCHEMA) + '."' +  \
                object_name + '"'

        return object_name, db_object_name

    def _generate_object_name(self, parts):
        """
        Generate a consistent object name generically

        Parameters
        ----------
        parts : list
            parts that will make up the name

        Returns
        -------
        object_name : str
            object name which can be used for internal reference

        """
        object_name = ''
        for idx, value in enumerate(parts):
            if not idx == 0:
                object_name += '_'
            object_name += StringUtils.remove_special_characters(value).lower()

        # Assure we max out the number characters of the objectname to 125 to keep
        # them readable (not purely by chance the max of HANA is 126)
        # Assuming with 125 characters sufficient uniqueness is achieved
        if len(object_name) > 125:
            object_name = object_name[:125]

        # If empty generate unique (unreadable) id
        if object_name == '':
            object_name = self._generate_unique_id()

        return object_name

    def _generate_unique_id(self):
        """
        Generate a unique id

        Returns
        -------
        hex : str
            unique id
        """
        return uuid.uuid4().hex

    def _get_last_index(self, items, str_to_check):
        """
        Get the last index of a string in a list.

        Returns
        -------
        last_index : int
            last index of the string to check
        """
        last_index = -1
        for idx, value in enumerate(items):
            if str_to_check in value:
                last_index = idx
        return last_index


class SqlProcessor(SqlProcessorBase):
    """
    This class contains the logic to translate the hana ml sql trace object
    to base layer objects and consumption layer objects. This can then
    be used to generated the solution (ie HANA or DataHub) specific artifacts.
    As the base layer is implemented by HANA artifacts this is specific catered
    for ease of generation of hana artifacts. The consumption layer objects are
    more abstract as different solution specific implementation of the consumption
    layer have different requirements.
    """

    def __init__(self, config, raise_on_error=False, log_raw_sql=False):
        """
        Sql processing class for artifact generation.

        Parameters
        ----------
        config : dict
            The config object holds the different configuration options required for
            generation.
        raise_on_error : boolean
            How to deal with errors. Whether it should be raised and dealt by the calling
            object or silently logged only.
        log_raw_sql: boolean
            Whether to log the raw sql provided by sql trace for easy debugging and validation
            of artifacts.
        """
        super(SqlProcessor, self).__init__(config)
        self._raise_on_error = raise_on_error
        self._log_raw_sql = log_raw_sql
        self._base_layer_generator = SqlProcessorBaseLayer(config)
        self._consumption_layer_generator = SqlProcessorConsumptionLayer(config)

    def parse_sql_trace(self, connection_context):
        """
        Entry point of Sql Trace processing.

        Parameters
        ----------
        connection_context : object
            The HANA ML connection object which holds the sql trace object
        """
        sql_trace = connection_context.sql_tracer.get_sql_trace()
        if sql_trace:
            self._process_sql(sql_trace)
        else:
            raise ValueError('No sql trace found. '
                             + 'Please assure you enable the trace before performing any'
                             + ' HANA ML API interactions. '
                             + 'ie: connection_context.sql_tracer.enable_sql_trace(True)')

    def _process_sql(self, sql_trace):
        """
        Entry point of Sql Trace processing. Results are stored in the config dict.

        Parameters
        ----------
        connection_context : object
            The HANA ML connection object which holds the sql trace object
        """
        sql_processed = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)
        if sql_trace:
            if self._log_raw_sql:
                logger.debug(self._get_raw_sql(sql_trace))

            # Process / Transform sql
            for algo in sql_trace:
                if algo and any(supported_algo.lower() in algo.lower() for supported_algo in
                                self.SUPPORTED_ALGOS):  # Check for None values
                    for function in sql_trace[algo]:
                        if sql_trace[algo][function]:  # Check for None values
                            if self.TRACE_KEY_SQL in sql_trace[algo][function]:
                                sql_functions, sql_entries, input_tables, output_tables, \
                                    output_vars = self._preprocess_sql(sql_trace[algo][function])
                                self._base_layer_generator.generate_base_layer(algo,
                                                                               function,
                                                                               sql_processed,
                                                                               sql_entries,
                                                                               sql_functions,
                                                                               input_tables,
                                                                               output_tables,
                                                                               output_vars)
                            else:
                                error_msg = 'No sql entries found for algorithm: {} ' \
                                            + 'and function {}'.format(algo, function)
                                if self._raise_on_error:
                                    raise ValueError(error_msg)
                                else:
                                    logger.error(error_msg)
                                    continue

            # Build generic consumption layer
            sql_processed[self.TRACE_KEY_CONSUMPTION_LAYER] = \
                self._consumption_layer_generator.generate_consumption_layer(sql_processed)

    def _preprocess_sql(self, trace_object):
        """
        Before commencing the actual sql trace processing it is preprocessed. This is required as
        HANA ML has a mix of autonomous sql block calls as well as direct individual sql
        statements. This method converts the autonomous sql blocks to single calls as to have a
        consistent way the different objects need to be generated.

        Parameters
        ----------
        trace_object : dict
            One individual traced object in the sql trace from HANA ML

        Returns
        -------
        sql_functions : list
            SQL functions used in the sql
        sql_entries : list
            SQL entries themselves generated by the hana ml package.
        input_tables: list
            Input tables used by the sql entries
        output_tables : list
            Output tables generated by the sql entries
        output_vars : list
            Output variables generated by the sql entries
        """
        input_tables = []
        output_tables = []
        output_vars = []
        sql_entries = []
        sql_functions = []
        if self.TRACE_KEY_FUNCTION in trace_object:
            sql_functions = trace_object[self.TRACE_KEY_FUNCTION]
        if self.TRACE_KEY_SQL in trace_object:
            sql_entries = trace_object[self.TRACE_KEY_SQL]
        if self.TRACE_KEY_TABLES_INPUT in trace_object:
            input_tables = trace_object[self.TRACE_KEY_TABLES_INPUT]
        if self.TRACE_KEY_TABLES_OUTPUT in trace_object:
            output_tables = trace_object[self.TRACE_KEY_TABLES_OUTPUT]
        if self.TRACE_KEY_VARS_OUTPUT in trace_object:
            output_vars = trace_object[self.TRACE_KEY_VARS_OUTPUT]

        # Determine type. Two currently:
        # 1. Traditional using single sql entries
        # 2. Autonomous block with multiple statements grouped together in 1
        # DO BEGIN / END statement
        is_autonomous = False
        if 'auto' in trace_object:
            is_autonomous = True

        if is_autonomous:
            # It is autonomous. Multiple rows are possibble. We combine them accordingly d
            # epending on hanaml version.
            # > 1.0.7
            sql_entry = StringUtils.flatten_string_array(sql_entries)
            # = 1.0.7
            if hanaml.__version__ == '1.0.7':
                sql_entry = sql_entries[0]
            # Sanity check
            if sql_entry:
                # As the auto generates multiple input / output table per autonymous call we
                # match it on the correct one based on the input select statement and can
                # ignore the rest.The input / output tables are grouped based on order. So each
                # new in table after a out table is a start of a new group. We check this by
                # checking that when we find an in if an out was found already we break the loop.
                # Hence the asumption is that we always have an in and an out for each
                # algo/function combination.
                in_done = False
                auto_in_tables = []
                auto_out_tables = []
                for entry in trace_object['auto']:
                    if 'auto_name' in entry:
                        if 'in_' in entry['auto_name']:
                            if in_done:
                                break
                            auto_in_tables.append(entry)
                        if 'out_' in entry['auto_name']:
                            auto_out_tables.append(entry)
                            in_done = True

                # We add the input tables to the input structure. Generally speaking no input
                # tables structure as this was not traced.
                for table in auto_in_tables:
                    input_table = copy.copy(table)
                    input_tables.append(input_table)

                # We process the output tables and assure we have a match to the traced output
                # tables. And change it accordingly and ignore any tables that do not have a
                # match to the  output_tables structure traced
                auto_output_tables = []
                for table in auto_out_tables:
                    # If the table exists in the output tables we adjust the naming
                    for out_table in output_tables:
                        if table[self.TRACE_KEY_TABLES_ATTRIB_NAME] == \
                                out_table[self.TRACE_KEY_TABLES_ATTRIB_NAME]:
                            output_table = copy.copy(out_table)
                            # Preserve original name
                            output_table['orig_name'] = \
                                output_table[self.TRACE_KEY_TABLES_ATTRIB_NAME]
                            output_table[self.TRACE_KEY_TABLES_ATTRIB_NAME] = table['auto_name']
                            # Preserve original select
                            output_table['orig_select'] = \
                                output_table[self.TRACE_KEY_TABLES_ATTRIB_SELECT]
                            # we adust the select accoringly
                            output_table[self.TRACE_KEY_TABLES_ATTRIB_SELECT].replace(
                                output_table['orig_name'], table['auto_name'])  # pylint: disable=line-too-long
                            auto_output_tables.append(output_table)

                # We need to make sure the output tables are properly mapped in the the
                # variables select
                for table in auto_output_tables:
                    if 'orig_name' in table:
                        for variable in output_vars:
                            if table['orig_name'] in variable[self.TRACE_KEY_ATTRIB_SELECT]:
                                # We are dealing with a temp table mapping which we need to remove from subsequent traced sql statements
                                variable[self.TRACE_KEY_ATTRIB_SELECT] = variable[self.TRACE_KEY_ATTRIB_SELECT].replace(
                                    '"' + table['orig_name'] + '"', ':' + table['name'])  # pylint: disable=line-too-long

                output_tables = auto_output_tables  # we overwrite with the output tables

                # To conform with original processing
                sql_entry = sql_entry.replace(';', '')
                # We split it to individuals entries in the list as to align with remaining processing
                sql_entries = sql_entry.split('\n')
                # Pre-filter elements which are not required:
                filtered_sql_entries = []
                for sql_entry in sql_entries:
                    # The in statements are generated as part of the input table processing
                    # and are not required.
                    if not sql_entry.startswith('in_'):
                        filtered_sql_entries.append(sql_entry)
                sql_entries = filtered_sql_entries

        return sql_functions, sql_entries, input_tables, output_tables, output_vars


class SqlProcessorBaseLayer(SqlProcessorBase):
    """
    This class deals with generating the base layer objects. This is completely catered
    for HANA generation as the base layer will also be in HANA.
    """

    def generate_base_layer(self, algo, function, sql_processed, sql_entries, sql_functions,
                            input_tables, output_tables, output_vars):
        """
        Start the generation of the base layer objects for this algo / function. Nothing is
        returned as the sql_processed is appended as required.

        Parameters
        ----------
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries. ie Score
        sql_processed : dict
            The object the conversion from sql trace
        sql_entries : list
            The generated sql entries by the HANA ML package
        sql_functions : list
            The different sql functions used. Mainly PAL functions
        input_tables : list
            The input or output variables of the that function
        output_tables : list
            Output tables generated by the sql entries
        output_vars : list
            Output variables generated by the sql entries
        """

        metadata = {}

        # Extend table structure with additional attriutes
        if input_tables:
            input_tables = self._extend_tables(algo, function, sql_entries, input_tables)
        if output_tables:
            output_tables = self._extend_tables(algo, function, sql_entries, output_tables)
        if output_vars:
            output_vars = self._extend_vars(algo, function, output_vars)

        # Build synonyms
        synonyms = self._build_synonyms(sql_functions, input_tables)
        sql_entries = self._filter_sql(sql_entries, input_tables)
        sql_entries = self._filter_sql(sql_entries, output_tables)
        sql_entries = self._filter_sql(sql_entries, output_vars)
        sql_entries = self._add_input_table_statements(sql_entries, input_tables)
        sql_entries = self._add_output_table_statements(sql_entries, output_tables)
        sql_entries = self._add_output_var_statements(sql_entries, output_vars)
        sql_entries = self._order_sql(sql_entries)
        sql_entries = self._transform_sql(sql_entries, synonyms)

        # Store processed structures
        if not self.TRACE_KEY_BASE_LAYER in sql_processed:
            sql_processed[self.TRACE_KEY_BASE_LAYER] = {}
        if not algo in sql_processed[self.TRACE_KEY_BASE_LAYER]:
            sql_processed[self.TRACE_KEY_BASE_LAYER][algo] = {}
        if not function in sql_processed[self.TRACE_KEY_BASE_LAYER][algo]:
            sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function] = {}

        # Generate metadata
        metadata[self.TRACE_KEY_METADATA_ATTRIB_PROC_NAME] = \
            'base_' + StringUtils.remove_special_characters(algo).lower() + '_' +  \
            StringUtils.remove_special_characters(function).lower()
        # TODO: Properly validate fit_predict functions to assure no fringe cases are missed
        metadata[self.TRACE_KEY_METADATA_ATTRIB_ALGO_FUNCTION_CAT] = \
            next((cat for cat in self.ALFO_FUNCTION_CATEGORIES if cat.lower() in
                  metadata[self.TRACE_KEY_METADATA_ATTRIB_PROC_NAME].lower()), None)

        if not metadata[self.TRACE_KEY_METADATA_ATTRIB_ALGO_FUNCTION_CAT]:
            metadata[self.TRACE_KEY_METADATA_ATTRIB_ALGO_FUNCTION_CAT] = 'Unknown'

        sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function][self.TRACE_KEY_SQL_PROCESSED] = sql_entries  # pylint: disable=line-too-long
        sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function][self.
                                                                 TRACE_KEY_TABLES_INPUT_PROCESSED] = input_tables  # pylint: disable=line-too-long
        sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function][self.
                                                                 TRACE_KEY_TABLES_OUTPUT_PROCESSED] = output_tables  # pylint: disable=line-too-long
        sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function][self.
                                                                 TRACE_KEY_VARS_OUTPUT_PROCESSED] = output_vars  # pylint: disable=line-too-long
        sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function][self.
                                                                 TRACE_KEY_SYNONYMS_PROCESSED] = synonyms  # pylint: disable=line-too-long
        sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function][self.
                                                                 TRACE_KEY_METADATA_PROCESSED] = metadata  # pylint: disable=line-too-long

    def _extend_tables(self, algo, function, sql_entries, tables):
        """
        Extending the tables from the HANA ML SQL trace with basic naming and convenience
        attributes.

        Parameters
        ----------
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries. ie Fit or Predict
        sql_entries : list
            The generated sql entries by the HANA ML package
        tables : list
            The input or output tables of the that function

        Returns
        -------
        extended_tables : list
            processed tables list with the extended attributes.
        """
        extended_tables = []
        for table in tables:
            extended_table = copy.copy(table)  # Clone object as to preserve original state
            # Add synonym select attribute
            if self.TRACE_KEY_TABLES_ATTRIB_NAME in extended_table:
                transformed_name = \
                    self._clean_string(extended_table[self.TRACE_KEY_TABLES_ATTRIB_NAME])
                extended_table[self.TRACE_KEY_TABLES_ATTRIB_TRNAME] = transformed_name
                # Parse type of table
                extended_table[self.TRACE_KEY_TABLES_ATTRIB_CAT] = \
                    next((cat for cat in self.TABLE_CATEGORIES if cat.lower() in
                          transformed_name.lower()), None)
                if not extended_table[self.TRACE_KEY_TABLES_ATTRIB_CAT]:
                    if 'orig_name' in extended_table:
                        # Auto scenario where the interpretation has to be done on the orig_name.
                        extended_table[self.TRACE_KEY_TABLES_ATTRIB_CAT] = \
                            next((cat for cat in self.TABLE_CATEGORIES if cat in
                                  extended_table['orig_name']), None)
                    else:
                        # Some cases no orig_name is known due to the way some tables are generated.
                        # So try if we can distill it from the table of the select
                        __, table = self._parse_dataset_sql(extended_table['select'])
                        if table:
                            extended_table[self.TRACE_KEY_TABLES_ATTRIB_CAT] = \
                                next((cat for cat in self.TABLE_CATEGORIES if cat in table), None)

                # Generate signature interface user friendly names
                if self.TRACE_KEY_TABLES_ATTRIB_CAT in extended_table and \
                        extended_table[self.TRACE_KEY_TABLES_ATTRIB_CAT]:
                    extended_table[self.TRACE_KEY_TABLES_ATTRIB_INT_NAME] = \
                        'lt_' + extended_table[self.TRACE_KEY_TABLES_ATTRIB_CAT].lower() + '_' +  \
                        extended_table[self.TRACE_KEY_TABLES_ATTRIB_TRNAME].lower() + '_' + \
                        StringUtils.remove_special_characters(algo).lower() + '_' + \
                        StringUtils.remove_special_characters(function).lower()
                else:
                    extended_table[self.TRACE_KEY_TABLES_ATTRIB_INT_NAME] = 'lt_' +  \
                        extended_table[self.TRACE_KEY_TABLES_ATTRIB_TRNAME].lower() + '_' + \
                        StringUtils.remove_special_characters(algo).lower() + '_' + \
                        StringUtils.remove_special_characters(function).lower()

                # Proposed dbobject name
                extended_table[self.TRACE_KEY_TABLES_ATTRIB_OBJECT_NAME], extended_table[self.
                                                                                         TRACE_KEY_TABLES_ATTRIB_DBOBJECT_NAME] = self._generate_db_object_name([algo,
                                                                                                                                                                 function, extended_table[self.TRACE_KEY_TABLES_ATTRIB_INT_NAME]])
                # Add schema for convenience
                extended_table[self.TRACE_KEY_TABLES_ATTRIB_SCHEMA] = \
                    self.config.get_entry(ConfigConstants.CONFIG_KEY_SCHEMA)

            # Only for output tables we would like to know if the table is part of the sql or not.
            # This to assure proper additional sql generation
            for sql_entry in sql_entries:
                if not self.TRACE_KEY_TABLES_ATTRIB_IN_SQL in extended_table:
                    extended_table[self.TRACE_KEY_TABLES_ATTRIB_IN_SQL] = False
                if not extended_table[self.TRACE_KEY_TABLES_ATTRIB_IN_SQL] and \
                        extended_table[self.TRACE_KEY_TABLES_ATTRIB_NAME] in sql_entry:
                    extended_table[self.TRACE_KEY_TABLES_ATTRIB_IN_SQL] = True
            extended_tables.append(extended_table)
        return extended_tables

    def _extend_vars(self, algo, function, variables):
        """
        Extending the variables from the HANA ML SQL trace with basic naming and convenience
        attributes.

        Parameters
        ----------
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries. ie Score
        input_tables : list
            The input or output variables of the that function

        Returns
        -------
        extended_vars : list
            processed vars list with the extended attributes.
        """
        extended_vars = []
        for variable in variables:
            extended_var = copy.copy(variable)  # Clone object as to preserve original state
            if self.TRACE_KEY_VARS_ATTRIB_NAME in extended_var:
                transformed_name = self._clean_string(extended_var[self.TRACE_KEY_VARS_ATTRIB_NAME])
                extended_var[self.TRACE_KEY_VARS_ATTRIB_TRNAME] = transformed_name

                # Parse type of table
                if self.TRACE_KEY_VARS_ATTRIB_TYPE in extended_var:
                    extended_var[self.TRACE_KEY_VARS_ATTRIB_CAT] = \
                        next((cat for cat in self.VAR_CATEGORIES if cat.lower() in
                              extended_var[self.TRACE_KEY_VARS_ATTRIB_TYPE].lower()), None)
                else:
                    extended_var[self.TRACE_KEY_VARS_ATTRIB_CAT] = \
                        next((cat for cat in self.VAR_CATEGORIES if cat.lower() in
                              transformed_name.lower()), None)

                # Generate signature interface user friendly names
                if self.TRACE_KEY_VARS_ATTRIB_CAT in extended_var and \
                        extended_var[self.TRACE_KEY_VARS_ATTRIB_CAT]:
                    extended_var[self.TRACE_KEY_VARS_ATTRIB_INT_NAME] = 'lv_' + \
                        extended_var[self.TRACE_KEY_VARS_ATTRIB_CAT].lower() + '_' +  \
                        extended_var[self.TRACE_KEY_VARS_ATTRIB_TRNAME].lower() + '_' + \
                        StringUtils.remove_special_characters(algo).lower() + '_' + \
                        StringUtils.remove_special_characters(function).lower()
                else:
                    extended_var[self.TRACE_KEY_VARS_ATTRIB_INT_NAME] = 'lv_' +  \
                        extended_var[self.TRACE_KEY_VARS_ATTRIB_TRNAME].lower() + '_' + \
                        StringUtils.remove_special_characters(algo).lower() + '_' + \
                        StringUtils.remove_special_characters(function).lower()
            extended_vars.append(extended_var)
        return extended_vars

    def _build_synonyms(self, sql_functions, input_tables):
        """
        As HDI containers only allow for synonym access to other HANA related artifacts
        we build up synonyms for sql function calls but also external data sources.

        Parameters
        ----------
        sql_functions : list
            The different sql functions used. Mainly PAL functions
        input_tables : list
            Input tables that are datasets used by the sql.

        Returns
        -------
        synonyms : list
            Generated synonyms
        """
        synonyms = []
        for function in sql_functions:
            function_synonym = self._generate_synonym_struct(function, self.TRACE_KEY_FUNCTION)
            if function_synonym:
                # We add the synonym to the object for further processing usage
                function[self.SYNONYM_STRUCT_KEY_SYNONYM] = \
                    function_synonym[self.SYNONYM_STRUCT_KEY_SYNONYM]
                # Synonym list append for a multi replace
                synonyms.append(function_synonym)
        for input_table in input_tables:
            dataset_synonym = self._generate_synonym_for_input(input_table)
            if dataset_synonym:
                input_table[self.SYNONYM_STRUCT_KEY_SYNONYM] = \
                    dataset_synonym[self.SYNONYM_STRUCT_KEY_SYNONYM]
                synonyms.append(dataset_synonym)
        return synonyms

    def _generate_synonym_for_input(self, input_table):
        """
        Build synononym for input tables. Specifically for datasets used.

        Parameters
        ----------
        input_table : dict
            Input table that is a dataset

        Returns
        -------
        synonym : dict
            Generated synonym
        """
        # Assure dict key is in the table dict
        if 'select' in input_table:
            # Assure we have a SELECT element in the string
            if 'select' in input_table['select'].lower():
                # Assure we are not dealing with a internal temp table select by checking
                # for the internal table indicator '#'
                if not '#' in input_table['select']:
                    schema, table = self._parse_dataset_sql(input_table['select'])
                    db_object = {
                        self.TRACE_KEY_NAME: table,
                        self.TRACE_KEY_SCHEMA: schema
                    }
                    synonym = self._generate_synonym_struct(db_object, self.TRACE_KEY_DATASET)
                    return synonym
        return None

    def _generate_synonym_struct(self, db_object=None, synonym_type=None):
        """
        Generate the internal synononym dict

        Parameters
        ----------
        db_object : dict
            db object details. Schema and name.

        Returns
        -------
        synonym : dict
            Generated synonym
        """
        if db_object:
            if self.TRACE_KEY_NAME in db_object and self.TRACE_KEY_SCHEMA in db_object:
                object_name, object_schema, object_synonym = \
                    self._generate_synonym(db_object[self.TRACE_KEY_NAME],
                                           db_object[self.TRACE_KEY_SCHEMA])
                synonym = {
                    self.SYNONYM_STRUCT_KEY_OBJECT: object_name,
                    self.SYNONYM_STRUCT_KEY_SCHEMA: object_schema,
                    self.SYNONYM_STRUCT_KEY_SYNONYM: object_synonym,
                    self.SYNONYM_STRUCT_KEY_TYPE: synonym_type
                }
                return synonym
        return None

    def _generate_synonym(self, name, schema):
        """
        Generate the actual synonym to be used in sql

        Parameters
        ----------
        name : str
            db object name.
        schema : str
            db object schema.

        Returns
        -------
        synonym : str
            Generated synonym string
        """
        clean_name = self._clean_string(name, full=True)
        clean_schema = self._clean_string(schema, full=True)
        if name and schema:
            return name, schema, clean_schema + '::' + clean_name

    def _add_input_table_statements(self, sql_entries, tables):
        """
        Generate the input table sql statements

        Parameters
        ----------
        sql_entries : list
            the sql entries generated by the hana ml package
        tables : list
            the input tables that need to be included in the sql entries.

        Returns
        -------
        sql_entries : list
            Processed sql entries.
        """
        for table in tables:
            sql_entry = table[self.TRACE_KEY_TABLES_ATTRIB_TRNAME] + \
                ' = SELECT * FROM ' + ':' + table[self.TRACE_KEY_TABLES_ATTRIB_INT_NAME]
            # Check on which index we need to insert the statement as this needs to be
            # under the 'DECLARE' statements. This is due to the autonomous sql
            insert_index = self._get_last_index(sql_entries, 'DECLARE') + 1
            if sql_entry:
                sql_entries.insert(insert_index, sql_entry)
        return sql_entries

    def _add_output_table_statements(self, sql_entries, tables):
        """
        Generate the output table sql statements

        Parameters
        ----------
        sql_entries : list
            the sql entries generated by the hana ml package
        tables : list
            the output tables that need to be included in the sql entries.

        Returns
        -------
        sql_entries : list
            Processed sql entries.
        """
        for table in tables:
            sql_entry = None
            if table[self.TRACE_KEY_TABLES_ATTRIB_IN_SQL]:
                sql_entry = table[self.TRACE_KEY_TABLES_ATTRIB_INT_NAME] + \
                    ' = SELECT * FROM ' + ':' + table[self.TRACE_KEY_TABLES_ATTRIB_TRNAME]
            if not table[self.TRACE_KEY_TABLES_ATTRIB_IN_SQL]:
                sql_entry = table[self.TRACE_KEY_TABLES_ATTRIB_INT_NAME] + \
                    ' = ' + table[self.TRACE_KEY_TABLES_ATTRIB_SELECT]
            if sql_entry:
                sql_entries.append(sql_entry)
        return sql_entries

    def _add_output_var_statements(self, sql_entries, variables):
        """
        Generate the output table sql statements

        Parameters
        ----------
        sql_entries : list
            the sql entries generated by the hana ml package
        variables : list
            the output variables that need to be included in the sql entries.

        Returns
        -------
        sql_entries : list
            Processed sql entries.
        """
        for variable in variables:
            sql_entry = None
            sql_entry = 'SELECT ( ' + variable[self.TRACE_KEY_VARS_ATTRIB_SELECT] + \
                ' ) INTO ' + variable[self.TRACE_KEY_VARS_ATTRIB_INT_NAME] + ' FROM DUMMY'
            if sql_entry:
                sql_entries.append(sql_entry)
        return sql_entries

    def _filter_sql(self, sql_entries, items):
        """
        Filter any unnecessary sql entries. For example create statements of input
        tables which is generated as part of the procedue signature.

        Parameters
        ----------
        sql_entries : list
            the sql entries generated by the hana ml package
        items : list
            the items to be filtered

        Returns
        -------
        filtered_entries : list
            filtered sql entries.
        """
        filtered_entries = []
        for sql_entry in sql_entries:
            if 'DROP' in sql_entry:
                continue
            if 'CREATE' in sql_entry:
                # Excluding input tables if available as these will be part of the procedure
                # interface
                if items and any(item[self.TRACE_KEY_ATTRIB_NAME] in
                                 sql_entry for item in items):
                    continue
            if sql_entry.startswith('SELECT'):
                # Exclude plain select statements generally for output vars as these will
                # be populated as part of the proc interface
                if items and any(item[self.TRACE_KEY_ATTRIB_SELECT] in
                                 sql_entry for item in items):
                    continue
            filtered_entries.append(sql_entry)
        return filtered_entries

    def _order_sql(self, sql_entries):
        """
        Reorder the sql entries to assure proper syntax for procedures.
        ie DECLARE statemetns at the top of the procedure.

        Parameters
        ----------
        sql_entries : list
            the sql entries generated by the hana ml package

        Returns
        -------
        reordered_entries : list
            ordered sql entries.
        """
        reordered_entries = []
        other_entries = []
        for sql_entry in sql_entries:
            if 'CREATE' in sql_entry:
                reordered_entries.append(sql_entry)
            else:
                other_entries.append(sql_entry)
        reordered_entries.extend(other_entries)
        return reordered_entries

    def _transform_sql(self, sql_entries, synonyms):
        """
        Transform the sql entries to assure proper syntax for procedures.
        ie table variables and <table_var>.INSERT to add data to parameter table.

        Parameters
        ----------
        sql_entries : list
            the sql entries generated by the hana ml package
        synonyms : list
            synonyms to replace the direct usage of schema/table/function to synonym usage.

        Returns
        -------
        transformed_entries : list
            transformed sql entries.
        """
        transformed_entries = []
        # Build synonym replacements
        synonym_replacements = self._build_synonym_replacements(synonyms)
        for sql_entry in sql_entries:
            replacements = {}

            if 'CREATE' in sql_entry:
                replacements.update({
                    'CREATE LOCAL TEMPORARY COLUMN TABLE': 'DECLARE',
                    '" (': ' TABLE (',
                    '"#': ''
                })

            if 'INSERT' in sql_entry:
                replacements.update({
                    'INSERT INTO ': '',
                    ' VALUES ': '.INSERT(',
                    ')': '))',
                    '"': '',
                    '#': ':'
                })

            if 'CALL' in sql_entry:
                replacements.update({
                    ' WITH OVERVIEW': '',
                    '"': '',
                    '#': ':'
                })

            if 'SELECT' in sql_entry:
                replacements.update({
                    '#': ':'
                })

            # Ignore anonymous block statement
            if sql_entry.startswith('DO') or sql_entry.startswith('BEGIN') or \
                    sql_entry.startswith('END'):
                continue

            # Merge dicts in the 'classical' way. Not using the option (merged_replacements
            # = {**replacements, **synonym_replacements}) of >3.5 on purpose for backwards
            # compatibility
            merged_replacements = replacements.copy()
            merged_replacements.update(synonym_replacements)

            sql_entry = StringUtils.multi_replace(sql_entry, merged_replacements)

            if not sql_entry == '':
                # Add sqlscript end statement identfier
                sql_entry = sql_entry + ';'
                transformed_entries.append(sql_entry)

        return transformed_entries

    def _build_synonym_replacements(self, synonyms):
        """
        Based on the synonyms build the replacement structure to support multi replacement in
        string.

        Parameters
        ----------
        synonyms : list
            synonyms to replace the direct usage of schema/table/function to synonym usage.

        Returns
        -------
        replacements : list
            replacments entries of the synonyms
        """
        replacements = {}
        for synonym in synonyms:
            if synonym['type'] == self.TRACE_KEY_FUNCTION:
                # There is difference how HANA ML Pythoon API deals with the call statements
                # to functions. Traditional function is quoted,
                # ie _SYS_AFL."PAL_RANDOM_DECISION_TREES", but in the auto (autonomous)
                # case the function is unquoted, ie _SYS_AFL.PAL_RANDOM_DECISION_TREES. To assure
                # we support both cases we provide both replacements.
                # traditional single sql entries
                replacements[synonym['schema'] + '."' + synonym['object'] + '"'] = '"' + \
                    synonym['synonym'] + '"'
                # Autonomous grouped sql entries
                replacements[synonym['schema'] + '.' + synonym['object']] = '"' + \
                    synonym['synonym'] + '"'
            if synonym['type'] == self.TRACE_KEY_DATASET:
                # There is difference how HANA ML Pythoon API deals with the table statements.
                # Traditional it is quoted, ie "SCHEMA"."TABLE", but in the auto (autonomous) case
                # the schema and table is unquoted, ie SCHEMA.TABLE. To assure we support both
                # cases we provide both replacements
                # Traditional single sql entries
                replacements['"' + synonym['schema'] + '"."' + synonym['object'] + '"'] = '"' + \
                    synonym['synonym'] + '"'
                # Autonomous grouped sql entries
                replacements[synonym['schema'] + '.' + synonym['object']] = '"' + \
                    synonym['synonym'] + '"'
        return replacements


class SqlProcessorConsumptionLayer(SqlProcessorBase):
    """
    This class deals with generating the consumption layer objects. This is generic as it needs
    to cater for multiple soluiton specific implementations.

    Based on generation type decides how to prepare the basic objects for the consumption layer

    Also merging and grouping is implemented here.
    """

    def generate_consumption_layer(self, sql_processed):
        """
        Start the generation of the consumption layer objects based on the base layer generated.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace

        Returns
        -------
        consumption_layer : dict
            Generated consumption layer
        """
        consumption_layer = []
        # We need to understand the relation between base_layer objects. So build up t
        # his context first and save for reference:
        sql_processed[self.TRACE_KEY_RELATION_CONTEXT] = \
            self._build_relation_context(sql_processed)
        # Based on grouping type grouping is set on the base_objects and passed tot he
        # consumption layer as the grouping implementation is on consumption layer level
        self._set_grouping(sql_processed)
        sql_proc_base_layer = sql_processed[self.TRACE_KEY_BASE_LAYER]
        for algo in sql_proc_base_layer:
            for function in sql_proc_base_layer[algo]:
                consumption_elements = self._build_consumption_layer_structure(
                    sql_processed, algo, function, self.TRACE_KEY_TABLES_ATTRIB_DBOBJECT_NAME)
                if consumption_elements:
                    consumption_layer.extend(consumption_elements)
        return consumption_layer

    def _build_relation_context(self, sql_processed):
        """
        Based on the base layer distill relationships between elements in the base layer.
        Generally speaking the output and input is validated where there is a relation.

        Note: Only the relationship between elements is defined. Not the relationship between
        the actual input and output tables / variables. This is validated and linked during
        consumption layer generation.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace

        Returns
        -------
        relations : list
            A list with relations
        """
        relations = []
        sql_proc_base_layer = sql_processed[self.TRACE_KEY_BASE_LAYER]
        for algo in sql_proc_base_layer:
            for function in sql_proc_base_layer[algo]:
                if self.TRACE_KEY_TABLES_OUTPUT_PROCESSED in sql_proc_base_layer[algo][function]:
                    # For each input table check wether the select of the output table is being
                    # used which indicates a relationship
                    for table in sql_proc_base_layer[algo][function][
                            self.TRACE_KEY_TABLES_OUTPUT_PROCESSED]:
                        select = table[self.TRACE_KEY_TABLES_ATTRIB_SELECT]
                        # check input structures
                        for check_algo in sql_proc_base_layer:
                            for check_function in sql_proc_base_layer[check_algo]:
                                if self.TRACE_KEY_TABLES_INPUT_PROCESSED in \
                                        sql_proc_base_layer[check_algo][check_function]:
                                    for check_table in sql_proc_base_layer[check_algo][
                                            check_function][
                                                self.TRACE_KEY_TABLES_INPUT_PROCESSED]:
                                        if select in  \
                                                check_table[self.TRACE_KEY_TABLES_ATTRIB_SELECT]:
                                            from_path = self.TRACE_KEY_BASE_LAYER + '/' \
                                                + algo + '/' + function
                                            from_metadata = sql_processed[
                                                self.TRACE_KEY_BASE_LAYER][algo][function][
                                                    self.TRACE_KEY_METADATA_PROCESSED]
                                            to_path = self.TRACE_KEY_BASE_LAYER + '/'  \
                                                + check_algo + '/' + check_function
                                            to_metadata = sql_processed[self.TRACE_KEY_BASE_LAYER][
                                                check_algo][check_function][self.
                                                                            TRACE_KEY_METADATA_PROCESSED]
                                            relation = {
                                                'from_path': from_path,
                                                'from_object': table,
                                                'from_metadata': from_metadata,
                                                'to_path': to_path,
                                                'to_object': check_table,
                                                'to_metadata': to_metadata
                                            }
                                            relations.append(relation)
        return relations

    def _set_grouping(self, sql_processed):
        """
        Add grouping identifiers to the processed elements. This is done by traversing the parents
        and children to see which group it should belong to.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace
        """
        group_strategy = self.config.get_entry(ConfigConstants.CONFIG_KEY_GROUP_STRATEGY)
        sql_proc_base_layer = sql_processed[self.TRACE_KEY_BASE_LAYER]
        if group_strategy == ConfigConstants.GENERATION_GROUP_FUNCTIONAL:
            for algo in sql_proc_base_layer:
                for function in sql_proc_base_layer[algo]:
                    base_object = sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function]
                    if not 'groups' in base_object or not base_object['groups']:
                        base_object['groups'] = []
                    group_type = ConfigConstants.GROUP_UKNOWN_TYPE
                    unique_id = self._generate_unique_id()
                    group_identifier = algo + '_' + function
                    if ConfigConstants.GROUP_FIT_TYPE in function.lower() and \
                            ConfigConstants.GROUP_PREDICT_TYPE in function.lower():
                        group_type = ConfigConstants.GROUP_FIT_PREDICT_TYPE
                    elif ConfigConstants.GROUP_FIT_TYPE in function.lower():
                        group_type = ConfigConstants.GROUP_FIT_TYPE
                    elif ConfigConstants.GROUP_PREDICT_TYPE in function.lower():
                        group_type = ConfigConstants.GROUP_PREDICT_TYPE
                    else:
                        continue

                    self._set_chain_group(base_object,
                                          sql_processed,
                                          algo,
                                          function,
                                          group_type,
                                          group_identifier,
                                          unique_id)
        else:
            # All elements have the same group id
            for algo in sql_proc_base_layer:
                for function in sql_proc_base_layer[algo]:
                    base_object = sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function]
                    group_type = ConfigConstants.GROUP_UKNOWN_TYPE
                    unique_id = self._generate_unique_id()
                    group_identifier = ConfigConstants.GROUP_IDENTIFIER_MERGE_ALL
                    self._set_group(base_object, group_type, group_identifier, unique_id)

    def _set_chain_group(self,
                         base_object,
                         sql_processed,
                         algo,
                         function,
                         group_type,
                         group_identifier,
                         uid):
        """
        Add grouping identifiers to the processed elements. This is done by traversing the
        parents and children to see which group it should belong to.

        Parameters
        ----------
        base_object : dict
            The base layer element.
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.
        group_type : str
            Which group type. Mainly Fit or Predict.
        group_identifier : str
            Humen readable identifier of the group
        uid : str
            Generated unique id
        """
        self._set_group(base_object, group_type, group_identifier, uid)
        parent_objects = self._get_parent_objects(sql_processed, algo, function)
        child_objects = self._get_child_objects(sql_processed, algo, function)
        if parent_objects:
            for parent in parent_objects:
                parent_base, parent_algo, parent_function = self._get_path_parts(parent['path'])
                parent_base_object = sql_processed[parent_base][parent_algo][parent_function]
                self._set_chain_group_ascending(parent_base_object, sql_processed, parent_algo,
                                                parent_function, group_type, group_identifier, uid)
        if child_objects:
            for child in child_objects:
                child_base, child_algo, child_function = self._get_path_parts(child['path'])
                child_base_object = sql_processed[child_base][child_algo][child_function]
                self._set_chain_group_decending(child_base_object, sql_processed, child_algo,
                                                child_function, group_type, group_identifier, uid)

    def _set_group(self, base_object, group_type, group_identifier, uid):
        """
        Setting the group by creating a group dict struct.

        Parameters
        ----------
        base_object : dict
            The base layer element.
        group_type : str
            Which group type. Mainly Fit or Predict.
        group_identifier : str
            Humen readable identifier of the group
        uid : str
            Generated unique id
        """
        group = {
            'type': group_type,
            'identifier': group_identifier,
            'uid': uid
        }

        if not 'groups' in base_object or not base_object['groups']:
            base_object['groups'] = []

        base_object['groups'].append(group)

    def _set_chain_group_ascending(self, base_object, sql_processed, algo,
                                   function, group_type, group_identifier, uid):
        """
        Ascend the chain to find elements that belong to the same group.

        Note: When we are in a predict group we ignore fit and when we are in a fit group we ignore
        predict. As to assure Proper merging of elements

        Parameters
        ----------
        base_object : dict
            The base layer element.
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.
        group_type : str
            Which group type. Mainly Fit or Predict.
        group_identifier : str
            Humen readable identifier of the group
        uid : str
            Generated unique id
        """

        if self._allow_adding_to_chain_group(base_object, group_type):
            self._set_group(base_object, group_type, group_identifier, uid)
        parent_objects = self._get_parent_objects(sql_processed, algo, function)
        if parent_objects:
            for parent in parent_objects:
                parent_base, parent_algo, parent_function = self._get_path_parts(parent['path'])
                parent_base_object = sql_processed[parent_base][parent_algo][parent_function]
                self._set_chain_group_ascending(parent_base_object,
                                                sql_processed,
                                                parent_algo,
                                                parent_function,
                                                group_type,
                                                group_identifier,
                                                uid)

    def _set_chain_group_decending(self, base_object, sql_processed, algo,
                                   function, group_type, group_identifier, uid):
        """
        Descend the chain to find elements that belong to the same group.

        Note: When we are in a predict group we ignore fit and when we are in a fit group we ignore predict. As to assure
        Proper merging of elements

        Parameters
        ----------
        base_object : dict
            The base layer element.
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.
        group_type : str
            Which group type. Mainly Fit or Predict.
        group_identifier : str
            Humen readable identifier of the group
        uid : str
            Generated unique id
        """
        if self._allow_adding_to_chain_group(base_object, group_type):
            self._set_group(base_object, group_type, group_identifier, uid)
        child_objects = self._get_child_objects(sql_processed, algo, function)
        if child_objects:
            for child in child_objects:
                child_base, child_algo, child_function = self._get_path_parts(child['path'])
                child_base_object = sql_processed[child_base][child_algo][child_function]
                self._set_chain_group_decending(child_base_object, sql_processed, child_algo,
                                                child_function, group_type, group_identifier, uid)

    def _allow_adding_to_chain_group(self, base_object, group_type):
        """
        When we are in a predict group we ignore fit and when we are in a fit group we ignore
        predict. As to assure Proper merging of elements

        Parameters
        ----------
        base_object: dict
            The base layer element.
        group_type : str
            Which group type. Mainly Fit or Predict.

        Returns
        -------
        allow_adding : boolean
            allow adding the object to the chain group

        """
        if group_type == ConfigConstants.GROUP_FIT_TYPE:
            if not base_object[self.TRACE_KEY_METADATA_PROCESSED][self.TRACE_KEY_METADATA_ATTRIB_ALGO_FUNCTION_CAT].lower() \
                    == ConfigConstants.GROUP_PREDICT_TYPE:
                return True
        if group_type == ConfigConstants.GROUP_PREDICT_TYPE:
            if not base_object[self.TRACE_KEY_METADATA_PROCESSED][self.TRACE_KEY_METADATA_ATTRIB_ALGO_FUNCTION_CAT].lower() \
                    == ConfigConstants.GROUP_FIT_TYPE:
                return True
        return False

    def _build_consumption_layer_structure(self, sql_processed, algo, function, link_type):
        """
        Build the the consumption layer elements. Merging logic is applied here depending on
        what type of merging is requested.

        Furthermore we have the notion of link_type:

        * interface_name : which is the internal variable name within a procedure
        * dbobject_name : which is the physical table which can be used.

        This allows us to decide for example when merging partition and predict tegether to use
        the interface_name (ie tabble variable) rather then persisting the result of the partition
        logic in a physical table and retrieving it in the predict logic.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.
        link_type : str
            Which group type. Mainly Fit or Predict.

        Returns
        -------
        consumption_elements : list
            The complete set of consumption elements generated from the base layer objects
        """
        consumption_elements = []
        base_object = sql_processed[self.TRACE_KEY_BASE_LAYER][algo][function]
        base_object_grouping = None
        if 'groups' in base_object:
            base_object_grouping = base_object['groups']

        merge_strategy = self.config.get_entry(ConfigConstants.CONFIG_KEY_MERGE_STRATEGY)

        # In case of a base object being processed already as child object we need to ignore to not process it twice.
        if 'cons_processed' in base_object:
            if base_object['cons_processed']:
                return None

        proc_name, input_items, body_items, output_items = \
            self._build_consumption_layer_element(base_object, sql_processed,
                                                  algo, function, link_type)

        # Check if we want to merge the partion in the related algo/function object:
        if merge_strategy == ConfigConstants.GENERATION_MERGE_PARTITION:
            parents = self._get_parent_objects(sql_processed, algo, function)
            # 1st check if this is a patition object. If so process and process it shields to achieve the merge
            if 'partition' in proc_name.lower():
                # If children then overwrite output to children output and assure to merge the partiion with the children.
                children = self._get_child_objects(sql_processed, algo, function)
                if children:
                    # We force the link between the different elements to use the interface which is the table variable rather then persiting the data
                    # Physically to the DB
                    child_link_type = self.TRACE_KEY_TABLES_ATTRIB_INT_NAME
                    # We want to preserve the original base_object data and we will copy the objects accordingly
                    # We ignore the the output of the par as we no long store / expose it due to the merge

                    # output_items = []
                    for child in children:
                        par_proc_name = copy.copy(proc_name)
                        par_input_items = copy.copy(input_items)
                        par_body_items = copy.copy(body_items)
                        child_base, child_algo, child_function = self._get_path_parts(child['path'])
                        child_base_object = sql_processed[child_base][child_algo][child_function]
                        child_proc_name, child_input_items, child_body_items, child_output_items = \
                            self._build_consumption_layer_element(child_base_object, sql_processed,
                                                                  child_algo, child_function, child_link_type)
                        par_proc_name += '_' + child_proc_name
                        # Input part of body as it is no longer exposed
                        par_body_items.extend(child_input_items)
                        par_body_items.extend(child_body_items)
                        # par_output_items.extend(child_output_items)

                        # Build consumption element per partition + child object
                        consumption_element = {'name': '', 'sql': {}}
                        consumption_element['name'] = 'cons_' + par_proc_name
                        consumption_element['sql']['input'] = par_input_items
                        consumption_element['sql']['body'] = par_body_items
                        # We ignore the output of the par as we no longe store / expose it
                        consumption_element['sql']['output'] = child_output_items
                        consumption_element['groups'] = base_object_grouping if base_object_grouping else [
                        ]
                        consumption_elements.append(consumption_element)
                        # Set child eleemtn as processed.
                        child_base_object['cons_processed'] = True

                    # Set partition object as processed
                    base_object['cons_processed'] = True
                    return consumption_elements

            # 2nd we check if it has parents. If it does we are not at the start and we ignore this element:
            elif parents:
                # Check if parent is not partition otherwise just process normally and
                # generation consumption element
                for parent in parents:
                    if 'partition' in parent['name']:
                        # Do not process this object yet but let it be processed as child of
                        # partition so it can be properly merged.
                        return None

        consumption_element = {}
        consumption_element['name'] = 'cons_' + proc_name
        consumption_element['display_name_long'] = algo.title() + ' ( ' + function.title() + ' )'
        consumption_element['display_name_short'] = function.title()
        consumption_element['algo'] = algo
        consumption_element['function'] = function
        consumption_element['sql'] = {}
        consumption_element['sql']['input'] = input_items
        consumption_element['sql']['body'] = body_items
        consumption_element['sql']['output'] = output_items
        consumption_element['groups'] = base_object_grouping if base_object_grouping else []
        base_object['cons_processed'] = True
        consumption_elements.append(consumption_element)
        return consumption_elements

    def _build_consumption_layer_element(
            self, base_object, sql_processed, algo, function, link_type):
        """
        Build the consumption layer element structure for each base object.

        Parameters
        ----------
        base_object : dict
            The base layer element.
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.
        link_type : str
            Which group type. Mainly Fit or Predict.

        Returns
        -------
        proc_name : str
            Procedure name of the consumption element
        input_items : list
            Input items, tables and variables of the consumption element
        body_items : list
            Body items, ie the content of the consumption element
        output_items : list
            Output items, tables and variables of the consumption element
        """
        input_items = []
        body_items = []
        output_items = []
        # Input
        if self.TRACE_KEY_TABLES_INPUT_PROCESSED in base_object:
            for table in base_object[self.TRACE_KEY_TABLES_INPUT_PROCESSED]:
                interface_name = table[self.TRACE_KEY_TABLES_ATTRIB_INT_NAME]
                category = table[self.TRACE_KEY_TABLES_ATTRIB_CAT]
                table_type = table[self.TRACE_KEY_TABLES_ATTRIB_TYPE]
                abap_type = self._build_abap_datatype(table_type)
                rel_objects = self._get_parent_objects_by_interface(
                    sql_processed, algo, function, interface_name)
                has_relation = False
                input_schema_table = ''
                input_schema_table_syn = None
                rel_object = None
                if rel_objects:
                    has_relation = True
                    # Input should only have 1 link otherwise issue upstream.
                    rel_object = rel_objects[0]
                    input_schema_table = rel_object[link_type]
                    if link_type == 'interface_name':
                        input_schema_table = ':' + input_schema_table
                else:
                    # We have an external dataset input. Process accordingly
                    select = table[self.TRACE_KEY_TABLES_ATTRIB_SELECT]
                    # Use synonym in case available
                    if table[self.TRACE_KEY_TABLES_ATTRIB_SYNONYM]:
                        input_schema_table_syn = '"' + \
                            table[self.TRACE_KEY_TABLES_ATTRIB_SYNONYM] + '"'
                    schema, table = self._parse_dataset_sql(select)
                    input_schema_table = '"' + schema + '"."' + table + '"'
                    # Track data_sources to allow mapping data sources by the user
                    self.config.get_entry(ConfigConstants.CONFIG_KEY_DATA_SOURCE_MAPPING)[
                        select] = select
                input_sql = interface_name + ' = select * from {};'
                input_variables = [input_schema_table]
                input_variables_syn = None
                if input_schema_table_syn:
                    input_variables_syn = [input_schema_table_syn]
                input_item = {
                    'cat': category,
                    'name': interface_name,
                    'interface_name': interface_name,  # for hdbprocedure
                    'table_type': table_type,  # for hdbprocedure
                    'abap_type': abap_type,
                    'hasrel': has_relation,
                    'relobject': rel_object,
                    'sql': input_sql,
                    'sql_vars': input_variables,
                    'sql_vars_syn': input_variables_syn
                }
                input_items.append(input_item)

        #  Output Tables
        if self.TRACE_KEY_TABLES_OUTPUT_PROCESSED in base_object:
            for table in base_object[self.TRACE_KEY_TABLES_OUTPUT_PROCESSED]:
                interface_name = table[self.TRACE_KEY_TABLES_ATTRIB_INT_NAME]
                dbobject_name = table[self.TRACE_KEY_TABLES_ATTRIB_DBOBJECT_NAME]
                dbobject_name_schema = table[self.TRACE_KEY_TABLES_ATTRIB_SCHEMA]
                object_name = table[self.TRACE_KEY_TABLES_ATTRIB_OBJECT_NAME]
                table_type = table[self.TRACE_KEY_TABLES_ATTRIB_TYPE]
                cds_type = self._build_cds_entity_datatype(table_type)
                abap_type = self._build_abap_datatype(table_type)
                category = table[self.TRACE_KEY_TABLES_ATTRIB_CAT]
                rel_objects = self._get_child_objects_by_interface(
                    sql_processed, algo, function, interface_name)
                output_sql = 'TRUNCATE TABLE {}; \n'
                output_sql += 'INSERT INTO {} SELECT * FROM :{};'
                output_variables = [dbobject_name, dbobject_name, interface_name]
                if rel_objects:
                    # Can have more than one rel_object.
                    output_item = {
                        'cat': category,
                        'name': interface_name,
                        'interface_name': interface_name,  # for hdbprocedure TODO decide name and interface name difference?
                        'dbobject_name': dbobject_name,
                        'dbobject_name_schema': dbobject_name_schema,
                        'object_name': object_name,
                        'table_type': table_type,
                        'cds_type': cds_type,
                        'abap_type': abap_type,
                        'hasrel': True,
                        'relobject': rel_objects,
                        'sql': output_sql,
                        'sql_vars': output_variables
                    }
                    output_items.append(output_item)
                else:
                    output_item = {
                        'cat': category,
                        'name': interface_name,
                        'interface_name': interface_name,  # for hdbprocedure TODO decide name and interface name difference?
                        'dbobject_name': dbobject_name,
                        'dbobject_name_schema': dbobject_name_schema,
                        'object_name': object_name,
                        'table_type': table_type,
                        'cds_type': cds_type,
                        'hasrel': False,
                        'abap_type': abap_type,
                        'relobject': None,
                        'sql': output_sql,
                        'sql_vars': output_variables
                    }
                    output_items.append(output_item)

        if self.TRACE_KEY_VARS_OUTPUT_PROCESSED in base_object:
            for variable in base_object[self.TRACE_KEY_VARS_OUTPUT_PROCESSED]:
                interface_name = variable[self.TRACE_KEY_VARS_ATTRIB_INT_NAME]
                data_type = variable[self.TRACE_KEY_VARS_ATTRIB_DATA_TYPE]
                category = variable[self.TRACE_KEY_VARS_ATTRIB_CAT]
                # For now no relationship is assumed
                output_item = {
                    'cat': category,
                    'name': interface_name,
                    'interface_name': interface_name,  # for hdbprocedure
                    'table_type': data_type  # TODO: Refactor attribute to generically to type only.
                }
                output_items.append(output_item)

        #  Function call
        function_call = 'CALL {}('
        for idx, item in enumerate(input_items):
            function_call += item['name'] + ' => :' + item['name']
            if idx + 1 < len(input_items): #pylint: disable=len-as-condition
                function_call += ', '
        if len(input_items) > 0 and len(output_items) > 0: #pylint: disable=len-as-condition
            function_call += ', '
        for idx, item in enumerate(output_items):
            function_call += item['name'] + ' => ' + item['name']
            if idx + 1 < len(output_items): #pylint: disable=len-as-condition
                function_call += ', '
        function_call += ');'
        proc_name = ''
        if self.TRACE_KEY_METADATA_PROCESSED in base_object:
            if self.TRACE_KEY_METADATA_ATTRIB_PROC_NAME in base_object[self.TRACE_KEY_METADATA_PROCESSED]:
                proc_name = base_object[self.TRACE_KEY_METADATA_PROCESSED] \
                    [self.TRACE_KEY_METADATA_ATTRIB_PROC_NAME]
        body_variables = [proc_name]
        body_item = {
            'name': proc_name,
            'sql': function_call,
            'sql_vars': body_variables
        }
        body_items.append(body_item)
        return proc_name, input_items, body_items, output_items

    def _get_child_objects_by_interface(self, sql_processed, algo, function, interface_name):
        """
        Retrieve child objects by interface name. This allows a fine grained relation to be defined.
        We use the relationship context to know which elements are related and then we check on
        interface name level which actual output table is related to which element input table.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.
        interface_name : str
            The internal variable name

        Returns
        -------
        rel_objects : list
            Related objects

        """
        path = self.TRACE_KEY_BASE_LAYER + '/' + algo + '/' + function
        rel_objects = []
        if self.TRACE_KEY_RELATION_CONTEXT in sql_processed:
            for relation in sql_processed[self.TRACE_KEY_RELATION_CONTEXT]:
                if relation['from_path'] == path and relation['from_object'][
                        self.TRACE_KEY_TABLES_ATTRIB_INT_NAME] == interface_name:
                    rel_object = {
                        'path': relation['to_path'],
                        'groups': self._get_attribute_from_path(sql_processed, relation['to_path'],
                                                                'groups'),
                        'direction': 'out',
                        'name': relation['to_metadata']['procedure_name'],
                        # consumption name as per the naming standard
                        'cons_name': 'cons_' + relation['to_metadata']['procedure_name'],
                        'interface_name': relation['to_object']['interface_name'],
                        'dbobject_name': relation['to_object']['dbobject_name']
                    }
                    rel_objects.append(rel_object)
        return rel_objects

    def _get_child_objects(self, sql_processed, algo, function):
        """
        Retrieve child objects by relationship context only.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.

        Returns
        -------
        rel_objects : list
            Related objects
        """
        path = self.TRACE_KEY_BASE_LAYER + '/' + algo + '/' + function
        rel_objects = []
        if self.TRACE_KEY_RELATION_CONTEXT in sql_processed:
            for relation in sql_processed[self.TRACE_KEY_RELATION_CONTEXT]:
                if 'from_path' in relation and 'from_object' in relation:
                    if relation['from_path'] == path:
                        rel_object = {
                            'path': relation['to_path'],
                            'groups': self._get_attribute_from_path(
                                sql_processed, relation['to_path'],
                                'groups'),
                            'direction': 'out', 'name': relation['to_metadata']['procedure_name'],
                            'interface_name': relation['to_object']['interface_name'],
                            'dbobject_name': relation['to_object']['dbobject_name']}
                        rel_objects.append(rel_object)
        return rel_objects

    def _get_parent_objects_by_interface(self, sql_processed, algo, function, interface_name):
        """
        Retrieve parent objects by interface name. This allows a fine grained relation to be defined.
        We use the relationship context to know which elements are related and then we check on
        interface name level which actual output table is related to which element input table.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.
        interface_name : str
            The internal variable name

        Returns
        -------
        rel_objects : list
            Related objects
        """
        path = self.TRACE_KEY_BASE_LAYER + '/' + algo + '/' + function
        rel_objects = []
        if self.TRACE_KEY_RELATION_CONTEXT in sql_processed:
            for relation in sql_processed[self.TRACE_KEY_RELATION_CONTEXT]:
                if relation['to_path'] == path and relation['to_object'][
                        self.TRACE_KEY_TABLES_ATTRIB_INT_NAME] == interface_name:
                    rel_object = {
                        'path': relation['from_path'],
                        'groups': self._get_attribute_from_path(sql_processed, relation['from_path'],
                                                                'groups'),
                        'direction': 'in',
                        'name': relation['from_metadata']['procedure_name'],
                        # consumption name as per the naming standard
                        'cons_name': 'cons_' + relation['from_metadata']['procedure_name'],
                        'interface_name': relation['from_object']['interface_name'],
                        'dbobject_name': relation['from_object']['dbobject_name']
                    }
                    rel_objects.append(rel_object)
        return rel_objects

    def _get_parent_objects(self, sql_processed, algo, function):
        """
        Retrieve parent objects by relationship context only.

        Parameters
        ----------
        sql_processed : dict
            The object the conversion from sql trace
        algo : str
            Algorithm of the sql entries. ie RandomForestClassifier
        function : str
            Function of the sql entries.

        Returns
        -------
        rel_objects : list
            Related objects
        """
        path = self.TRACE_KEY_BASE_LAYER + '/' + algo + '/' + function
        rel_objects = []
        if self.TRACE_KEY_RELATION_CONTEXT in sql_processed:
            for relation in sql_processed[self.TRACE_KEY_RELATION_CONTEXT]:
                if 'to_path' in relation and 'to_object' in relation:
                    if relation['to_path'] == path:
                        rel_object = {
                            'path': relation['from_path'],
                            'groups': self._get_attribute_from_path(
                                sql_processed, relation['from_path'],
                                'groups'),
                            'direction': 'in',
                            'name': relation['from_metadata']['procedure_name'],
                            'interface_name': relation['from_object']['interface_name'],
                            'dbobject_name': relation['from_object']['dbobject_name']}
                        rel_objects.append(rel_object)
        return rel_objects

    def _build_cds_entity_datatype(self, hana_table_type):
        # TODO: look at moving this to hana generation
        """
        Convert a hana table type to a cds entity type

        Parameters
        ----------
        hana_table_type : str
            The hana table type that is provided by the hana ml trace object

        Returns
        -------
        cds_type : str
            CDS type generated based on hana table type
        """
        # Validate we have a proper table type. Single non-table outputs are combined.
        # TODO: Check whether to seperate single output versus tabble outputs.
        if ',' in hana_table_type:
            indent = '      '
            cds_data_type = ''
            # add whitespace at the end to distinguish between end ) versus datatype
            # size defintion (ie. VARCHAR(21))
            hana_table_type += ' '
            replacements = {
                'table (': '',
                ') ': '',
                '"': ''
            }
            clean_hana_table_types_str = StringUtils.multi_replace(hana_table_type, replacements)
            clean_hana_table_types = clean_hana_table_types_str.split(',')
            data_type_mapping = self.config.get_entry(
                ConfigConstants.CONFIG_KEY_HDBTABLE_TO_HDBDD_TYPES)
            for data_type in clean_hana_table_types:
                column_name, column_type = data_type.split(' ')
                if '(' in column_type:
                    column_data_type = column_type[:column_type.index('(')]
                    column_size = column_type[column_type.index('('):]
                    cds_data_type += indent + column_name + ' : ' + \
                        data_type_mapping[column_data_type] + column_size + ';\n'
                else:
                    cds_data_type += indent + column_name + ' : ' + \
                        data_type_mapping[column_type] + ';\n'
            return cds_data_type
        return None

    def _build_abap_datatype(self, hana_table_type):
        # TODO: look at moving this to amdp generation
        """
        Convert a hana table type to a abap data type

        Parameters
        ----------
        hana_table_type : str
            The hana table type that is provided by the hana ml trace object

        Returns
        -------
        abap_data_type : str
            ABAP data type generated based on hana table type
        """
        type_str = ' TYPE '
        length_str = ' LENGTH '
        abap_data_type = ''
        # add whitespace add the end to distinguish between end ) versus datatype size
        # defintion (ie. VARCHAR(21))
        hana_table_type += ' '
        replacements = {
            'table (': '',
            ') ': '',
            '"': ''
        }
        clean_hana_table_types_str = StringUtils.multi_replace(hana_table_type, replacements)
        clean_hana_table_types = clean_hana_table_types_str.split(',')
        data_type_mapping = self.config.get_entry(ConfigConstants.CONFIG_KEY_HDBTABLE_TO_ABAP_TYPES)
        for data_type in clean_hana_table_types:
            column_name, column_type = data_type.split(' ')
            if '(' in column_type:
                column_data_type = column_type[:column_type.index('(')]
                column_size = column_type[column_type.index('('):].replace("(", "").replace(")", "")
                abap_data_type += column_name + type_str + \
                    data_type_mapping[column_data_type] + length_str + column_size + ',\n'
            else:
                abap_data_type += column_name + type_str + data_type_mapping[column_type] + ',\n'
        return abap_data_type
