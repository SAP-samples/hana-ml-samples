"""
This module handles generation of all DataJub related artifacts based on the provided
consumption layer elements. Currenlty the functionality builds up a graph json which
can be further amended in the modeler if required. 
"""
import os
import time
from ..config import ConfigConstants
from ..hana_ml_utils import DirectoryHandler
from ..hana_ml_utils import StringUtils
from .filewriter.datahub import GraphWriter 

from ..sql_processor import SqlProcessor

class DataHubGenerator(object):
    """
    This class provides DataHub specific generation functionality. It also extend the config
    to cater for DatHub generation specific config.
    """
    def __init__(self, config):
        """
        This is main entry point for generating the DataHub related artifacts.

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.directory_handler = DirectoryHandler()
        self.config = config
        self._extend_config()

    def generate_artifacts(self, include_rest_endpoint=False, include_ml_operators=False):
        """
        Generate the artifacts by first building up the required folder structure for artifact storage and then 
        generating the different required files. DataHub can be used by itself or in conjunction with a SAP DI scenario
        where ML API specific operators are generated which are part of the SAP DI solution.

        Parameters
        ----------
        include_rest_endpoint: boolean
            Include a rest endpoint. Normal SAP DI usage needs a rest endpoint.
        include_ml_operators: boolean
            Include ML operators for the SAP DI scenario
        
        Returns
        -------
        output_path : str
            Return the output path of the root folder where the hana related artifacts are stored.
        """
        self._build_folder_structure()
        consumption_processor = DataHubConsumptionProcessor(self.config)
        consumption_processor.generate(self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_DATAHUB), include_rest_endpoint, include_ml_operators)
        return self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_DATAHUB )

    def _build_folder_structure(self):
        """
        Build up the folder structure. It is currenlty not a deep structure but just a subbfolder datahub
        under the root output path.
        """
        self._clean_folder_structurre()
        # Create base directories
        self.directory_handler.create_directory( self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_DATAHUB ))
        
    def _clean_folder_structurre(self):
        """
        Clean up physical folder structure. 
        """
        path = self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_DATAHUB )
        if os.path.exists( path ):
            self.directory_handler.delete_directory_content( path )
            os.rmdir( path )

    def _extend_config(self):
        """
        Extend the config to cater for DatHub generation specific config.
        """
        output_path_datahub = os.path.join(self.config.get_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH ), ConfigConstants.DATAHUB_BASE_PATH )
        self.config.add_entry( ConfigConstants.CONFIG_KEY_OUTPUT_PATH_DATAHUB, output_path_datahub)

class DataHubConsumptionProcessor(object):
    """
    This class provides DataHub specific generation functionality for the datahub graphs. 
    It utilizes the consumption layer to generate the graph json. 
    
    When looking at the consumption layer element the following high level mapping can be
    made to a datahub operator:

    * Consumption Element = (Python) Operator
    * Input Table / Input Variable = Input port
    * Output Table / Output Table = Output port

    However more operators are generated to cater for different use cases. For example it is
    possible te gnerate a rest endpoint in the graph which requires additional operators and
    connections to be supported. Same holds for the SAP DI scenario. 

    Further more the  DataHub generator requires the groups functionality to adhere
    to sap di best practices of sperating the fit and predict as 2 seperate graphs. 
    A group is a collection of consumption layer elements that need to be combined in 1 graph.
    An example is the partition function which can be generated as a single operator on a 
    datahub graph. From this single operator a connection can be made to a fit and a
    predict operator to provide the data split in train and test sets for the function call. For
    SAP DI the fit and predict process needs to be split and hence the partition operator 
    needs to be generated in both graphs and hence fit to 2 groups, fit and predict. In other
    words we have a single consumption element which is part of two logical groups of consumption
    elements, or in datahub terms, operators.

    The grouping functionality was build sap di in mind, but has been setup generically to be able
    to cater for different cases. 
    """
    def __init__(self, config):
        """
        This class allows to generate the arifacts for the DataHub graph. 

        Parameters
        ----------
        config : dict
            Central config object
        """
        self.config = config
        self.operator_tracker = {}

    def generate(self, path, include_rest_endpoint=False, include_ml_operators=False):
        """
        Method for generating the actual artifacts content.  

        Parameters
        ----------
        path: str
            The physical location where to store the artifacts. 
        include_rest_endpoint: boolean
            Include a rest endpoint. Normal SAP DI usage needs a rest endpoint.
        include_ml_operators: boolean
            Include ML operators for the SAP DI scenario
        """
        graph_writer = GraphWriter(self.config)

        sql_processed_cons_layer = self.config.get_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED)[SqlProcessor.TRACE_KEY_CONSUMPTION_LAYER]
        
        sql_key_sql = SqlProcessor.TRACE_KEY_SQL_PROCESSED

        self.graphs = {}
        
        for element in sql_processed_cons_layer:
            if not isinstance(element, dict):
                continue # ignore TODO: proper doc
            
            # Generate consumption layer sql    
            if sql_key_sql in element: # TODO: gen warning if no sql
                connections = {}
                groups = []
                if 'groups' in element:
                    if element['groups']:
                        groups = element['groups']
                operators = []
                operator_id = element['name']
                operator_display_name = element['display_name_short']

                input = []
                output = []
                body = []
                if 'input' in element[sql_key_sql]:
                    input = element[sql_key_sql]['input']
                if 'body' in element[sql_key_sql]:
                    body = element[sql_key_sql]['body']
                if 'output' in element[sql_key_sql]:
                    output = element[sql_key_sql]['output']
                
                # Build SQL & Ports arrays
                sql = []
                in_ports = []
                out_ports = []
                out_ports_values = []
                target_schema = self.config.get_entry(ConfigConstants.CONFIG_KEY_SCHEMA)
                for item in input:
                    sql_str = ''
                    if item['hasrel']:
                        in_ports.append(self._generate_port(item['interface_name']))
                        sql_str = item[sql_key_sql]
                    else:
                        sql_str = item[sql_key_sql].format(*item['sql_vars'])
                    sql.append(sql_str)
                    if include_ml_operators:
                        if self.config.is_model_category(item['cat']) and not element['function'] == 'Score': 
                            # Generate Blob Converter and Model Producer
                            mdl_cons_id = operator_id + '_mdlcons'
                            mdl_name = 'com.sap.hanaml.' + self.config.get_entry(ConfigConstants.CONFIG_KEY_PROJECT_NAME) + '_mdl'
                            str_converter_id = operator_id + '_strconv'
                            mld_cons_ops, mdl_cons_conns = self._generate_mlapi_model_operator(mdl_name, ConfigConstants.DATAHUB_MLAPI_OPERATOR_MDL_CONS_COMPONENT,mdl_cons_id, 'Model Consumer')
                            str_converter_op = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_CONVERTER_TOSTRING_COMPONENT, str_converter_id, 'To String', None, None, None, None, None)
                            # Generate connections
                            # From Constant Generator to ML-Api consumer
                            self._add_connections(groups, connections, mdl_cons_conns)
                            # From  ML-Api consumer to String Converter
                            self._add_connections(groups, connections, self._generate_port_connection(mdl_cons_id, ConfigConstants.DATAHUB_OPERATOR_MDL_CONS_OUT_PORT_BLOB, str_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOSTRING_IN_PORT_BLOB))
                            # From String Converter to Operator
                            self._add_connections(groups, connections, self._generate_port_connection(str_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOSTRING_OUT_PORT_STRING, operator_id, item['interface_name']))
                            # Add operators
                            operators.extend(mld_cons_ops)
                            operators.append(str_converter_op)

                for item in body:
                    proc_name = item['sql_vars'][0]
                    proc_name = target_schema + '."' + proc_name.upper() + '"'
                    sql_str = item[sql_key_sql].format(proc_name)
                    sql.append(sql_str)
                for item in output:
                    # Check if we have relation defined. In case of a 1 to many relationship we need to assure 
                    # we duplicate the port as datahub does not [yet] support 1 to many port relationships
                    interface_name = item['interface_name']
                    if 'hasrel' in item and item['hasrel'] and  'relobject' in item and item['relobject']:
                        # First we generate the connections to all the related objects
                        idx_tracker = 0
                        for idx, relobject in enumerate(item['relobject']):
                            idx_tracker = idx
                            hasrel_interface_name = interface_name + str(idx_tracker)
                            out_ports.append(self._generate_port(hasrel_interface_name))
                            out_ports_values.append(self._generate_outport_value(hasrel_interface_name, target_schema, item['dbobject_name']))
                            self._add_connections(relobject['groups'], connections, self._generate_port_connection(operator_id, hasrel_interface_name, relobject['cons_name'], relobject['interface_name']))
                        # We add an index to create an additional port for normal processing
                        interface_name += str(idx_tracker+1)

                     # We process the output as normal to assure proper generation of requested opperators is done such as ml api or restpoint operators. 
                    out_ports.append(self._generate_port(interface_name))
                    if 'dbobject_name' in item:
                        out_ports_values.append(self._generate_outport_value(interface_name, target_schema, item['dbobject_name']))
                    self._generate_output_content(include_rest_endpoint, include_ml_operators, groups, operator_id, interface_name, item, element, operators, connections)
                    
                    # TODO: Refactor this
                    if 'sql_vars' in item:
                        sql_var_dbobject = target_schema + '.' + item['sql_vars'][0]
                        item['sql_vars'][0] = sql_var_dbobject
                        item['sql_vars'][1] = sql_var_dbobject
                        sql_str = item[sql_key_sql].format(*item['sql_vars'])
                        sql.append(sql_str)
                    
                python_script = self._generate_python_script(sql, in_ports, out_ports_values)
                operator = self._generate_operator(ConfigConstants.DATAHUB_HANAML_OPERATOR_COMPONENT, operator_id, operator_display_name, None, python_script, in_ports, out_ports, None)
                operators.append(operator)
                if 'groups' in element:
                    if element['groups']:
                        for group in element['groups']:
                            if not group['identifier'] in self.graphs:
                                self.graphs[group['identifier']] = {}
                            if not 'terminator_id' in self.graphs[group['identifier']]:
                                self.graphs[group['identifier']]['terminator_id'] = None
                            if not 'operators' in self.graphs[group['identifier']]:
                                self.graphs[group['identifier']]['operators'] = []
                            if not 'connections' in self.graphs[group['identifier']]:
                                self.graphs[group['identifier']]['connections'] = []
                            self.graphs[group['identifier']]['operators'].extend(operators)
                            if connections:
                                if group['identifier'] in connections:
                                    self.graphs[group['identifier']]['connections'].extend(connections[group['identifier']])
        
        for graph_identifier in self.graphs:
            graph_operators = self.graphs[graph_identifier]['operators']
            graph_connections = self.graphs[graph_identifier]['connections']
            graph = self._generate_graph("", graph_operators, graph_connections)
            graph_writer.generate(path, graph_identifier, graph) 

    
    def _get_graph_terminator_operator_id(self, group_identifier):
        """
        In case of sap di ml operators we include a graph terminator for the fit aka train
        graph to abide by the sap di proposed best practise for these graphs. To fit this is
        dynamic build of the graph we have a terminator operator per graph of which we need
        to retrieve the id to be able to generate operator connections to the singel graph
        terminator.  

        Parameters
        ----------
        group_identifier: str
            The physical location where to store the artifacts. 
        
        Returns
        -------
        terminator_id : str
            The terminator operator id within the graph
        """
        terminator_op_id = None
        if group_identifier in self.graphs:
            if 'terminator_id' in self.graphs[group_identifier]:
                terminator_op_id = self.graphs[group_identifier]['terminator_id']
        return terminator_op_id

    def _add_graph_terminator_connection(self, operator_id, groups, operators, connections, src_process, src_port, tgt_port):
        """
        In case of sap di ml operators we include a graph terminator for the fit aka train
        graph to abide by the sap di proposed best practise for these graphs. Here the 
        existence of the terminator operator is checked and otherwise created before
        the connection is generated.

        Parameters
        ----------
        operator_id: str
            The operator id is the link to the consumption layer element id. This is extended
            to provide ids to related operators.
        groups : dict
            The groups the consumption layer element is part of
        operators : list
            The list of already generated operators as to extend this with the generated operators. 
        connections : list
            The connections list for this graph which will be appended with the new connection
        src_process : str
            The source operator from which the connection starts
        src_port : str
            The source oeprator port from which the connection starts
        tgt_port : str
            The target operator port to where the connection ends
        """
        for group in groups:
            # Check on graph level if terminator exists
            terminator_op_id = self._get_graph_terminator_operator_id(group['identifier'])
            if not terminator_op_id:
                if not group['identifier'] in self.graphs:
                    self.graphs[group['identifier']] = {} 
                if not 'terminator_id' in self.graphs[group['identifier']]:
                    terminator_op_id = operator_id + '_pyfin'
                    term_id = operator_id + '_terminator'
                    py_finish_op = self._generate_python_graph_finished_operator(terminator_op_id,'Finalize')
                    term_op = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_GRAPH_TERMINATOR_COMPONENT, term_id, 'Graph Terminator', None, None, None, None, None)
                    # From python terminator op to graph terminator 
                    self._add_connections(groups, connections, self._generate_port_connection(terminator_op_id, ConfigConstants.DATAHUB_OPERATOR_TERMINATOR_OUT_PORT_MESSAGE, term_id, ConfigConstants.DATAHUB_OPERATOR_GRAPH_TERMINATOR_IN_PORT_ANY))
                    operators.append(py_finish_op)
                    operators.append(term_op)
                    self.graphs[group['identifier']]['terminator_id'] = terminator_op_id
            self._add_connection(group['identifier'], connections, self._generate_port_connection(src_process, src_port, terminator_op_id, tgt_port))
           

    def _add_connections(self, groups, connections, new_connection):
        """
        Add a new connections in all the groups that the consumption layer element 
        belongs to

        Parameters
        ----------
        groups : dict
            The groups the consumption layer element is part of
        connections : list
            The connections list for this graph which will be appended with the new connection
        new_connection : dict
            The new connection that needs to be added
        """
        for group in groups:
            self._add_connection(group['identifier'], connections, new_connection)

    def _add_connection(self, group_identifier, connections, new_connection):
        """
        Add a new connection

        Parameters
        ----------
        group_identifier : str
            The group where the connections needs to be added
        connections : list
            The connections list for this graph which will be appended with the new connection
        new_connection : dict
            The connection that needs to be added
        """
        if not group_identifier in connections:
           connections[group_identifier] = []
        if  isinstance(new_connection, list):
           connections[group_identifier].extend(new_connection)    
        else: 
           connections[group_identifier].append(new_connection)
        

    def _generate_output_content(self, include_rest_endpoint, include_ml_operators, groups, operator_id, interface_name, item, element, operators, connections):
        """
        Depending on the different use cases generate additional output content, for example operators,
        are required. Two scenarios are currently supported:

        * SAP DI (ML Operators)
        * REST Endpoint for easy external exposure

        Parameters
        ----------
        include_rest_endpoint: boolean
            Include a rest endpoint. Normal SAP DI usage needs a rest endpoint.
        include_ml_operators: boolean
            Include ML operators for the SAP DI scenario
        groups : dict
            The groups the consumption layer element is part of
        operator_id: str
            The operator id is the link to the consumption layer element id. This is extended
            to provide ids to related operators.
        interface_name : str
            The interface name of the consumption layer element
        item : dict
            The input or output table of the 
        operators : list
            The list of already generated operators as to extend this with the generated operators. 
        connections : list
            The connections list for this graph which will be appended with the new connection
        """
        if include_rest_endpoint:
            if self.config.is_fitted_category(item['cat']) and not element['function'] == 'Score':
                basePath = self._get_rest_endpoint_path(item['cat'])
                if include_ml_operators:
                    basePath = '${deployment}'
                rest_operators, rest_connections = self._generate_rest_endpoint_operators(operator_id, item['interface_name'], basePath)
                if rest_operators and rest_connections:
                    operators.extend(rest_operators)
                    self._add_connections(groups, connections, rest_connections)

        if include_ml_operators:
            # Python Terminator operator
            # Generated as part of the model producer flow and used by metrics flow
            if self.config.is_model_category(item['cat']):
                # Generate Blob Converter and Model Producer
                blob_converter_id = operator_id + '_blobconv'
                mdl_prod_id = operator_id + '_mdlprod'
                mdl_name = 'com.sap.hanaml.' + self.config.get_entry(ConfigConstants.CONFIG_KEY_PROJECT_NAME) + '_mdl'
                msg_converter_id = operator_id + '_msgconv'
                str_converter_id = operator_id + '_strconv'
                blob_converter_op = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_CONVERTER_TOBLOB_COMPONENT, blob_converter_id, 'To Blob', None, None, None, None, None)
                mld_prod_ops, __ = self._generate_mlapi_model_operator(mdl_name, ConfigConstants.DATAHUB_MLAPI_OPERATOR_MDL_PROD_COMPONENT,mdl_prod_id, 'Model Producer')
                msg_conv_op = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_CONVERTER_TOMESSAGE_COMPONENT, msg_converter_id, 'To Message', None, None, None, None, None)
                str_conv_op = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_CONVERTER_TOSTRING_COMPONENT, str_converter_id, 'To String', None, None, None, None, None)
                # Generate connections
                # From Operator to Blob Converter
                self._add_connections(groups, connections, self._generate_port_connection(operator_id, interface_name, blob_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOBLOB_IN_PORT_STRING))
                # From Blob Converter to ML-Api model producer
                self._add_connections(groups, connections, self._generate_port_connection(blob_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOBLOB_OUT_PORT_BLOB, mdl_prod_id, ConfigConstants.DATAHUB_OPERATOR_MDL_PROD_IN_PORT_BLOB))
                # From ML-Api model producer to message converter
                self._add_connections(groups, connections, self._generate_port_connection(mdl_prod_id, ConfigConstants.DATAHUB_OPERATOR_MDL_PROD_OUT_PORT_MSG, msg_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOMESSAGE_IN_PORT_ANY))
                # From message converter to string converter
                self._add_connections(groups, connections, self._generate_port_connection(msg_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOMESSAGE_OUT_PORT_MESSAGE, str_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOSTRING_IN_PORT_MESSAGE))
                # From string converter to python terminator op to check when ready
                # self._add_connections(groups, connections, self._generate_port_connection(str_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOSTRING_OUT_PORT_STRING, self.terminator_ops_py_id, ConfigConstants.DATAHUB_OPERATOR_TERMINATOR_IN_PORT_STRING))
                self._add_graph_terminator_connection(operator_id, groups, operators, connections, str_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOSTRING_OUT_PORT_STRING, ConfigConstants.DATAHUB_OPERATOR_TERMINATOR_IN_PORT_STRING)
                # Add operators
                operators.append(blob_converter_op)
                operators.extend(mld_prod_ops)
                operators.append(msg_conv_op)
                operators.append(str_conv_op)
            if self.config.is_metric_category(item['cat']):
                # Add following for score result
                # api.send('output', str(accuracy))
                # metrics_dict = {"Accuracy": accuracy}
                ##send the metrics to the output port - Submit Metrics operator will use this to persist the metrics 
                # api.send("metrics", api.Message(metrics_dict))
                
                msg_converter_id = operator_id + '_msgconv'
                metrics_id = operator_id + '_metrics'
                msg_converter_op = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_CONVERTER_TOMESSAGE_COMPONENT,msg_converter_id,'To Message',None, None,None,None,None)
                metrics_op = self._generate_operator(ConfigConstants.DATAHUB_MLAPI_OPERATOR_METRICS_COMPONENT,metrics_id,'Submit Metrics',None, None,None,None,None)
                # Generate connections
                # From Operator to Blob Converter
                self._add_connections(groups, connections, self._generate_port_connection(operator_id, interface_name, msg_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOMESSAGE_IN_PORT_STRING))
                # From Blob Converter to ML-Api metric producer
                self._add_connections(groups, connections, self._generate_port_connection(msg_converter_id, ConfigConstants.DATAHUB_OPERATOR_TOMESSAGE_OUT_PORT_MESSAGE, metrics_id, ConfigConstants.DATAHUB_OPERATOR_METRICS_IN_PORT_METRICS))
                # From ML-Api metrics producer to python terminator op 
                # self._add_connections(groups, connections, self._generate_port_connection(metrics_id, ConfigConstants.DATAHUB_OPERATOR_METRICS_OUT_PORT_RESPONSE, self.terminator_ops_py_id, ConfigConstants.DATAHUB_OPERATOR_TERMINATOR_IN_PORT_MESSAGE))
                self._add_graph_terminator_connection(operator_id, groups, operators, connections, metrics_id, ConfigConstants.DATAHUB_OPERATOR_METRICS_OUT_PORT_RESPONSE, ConfigConstants.DATAHUB_OPERATOR_TERMINATOR_IN_PORT_MESSAGE)
                # Add operators
                operators.append(msg_converter_op)
                operators.append(metrics_op)


    def _generate_outport_value(self, interface_name, target_schema, dbobject_name):
        """
        As data is moved out of HANA only the schema/table name in HANA is transferred across
        operators.  This method generates a internal dict with this reference tbat is used
        as part of the python script generation.

        Parameters
        ----------
        interface_name : str
            The interface name of the consumption layer element
        target_schema : str
            The target schema where the dbobject_name resides
        dbobject_name : str
            The actual dbobject_name such as a table

        Returns
        -------
        out_port_value : dict
            Internal dict for easy consumption of this data 
        
        """
        out_port_value = {
            'port_name': interface_name,
            'dbobject_name': target_schema + '.' + dbobject_name
        }
        return out_port_value

    def _generate_python_script(self, sql, in_ports, out_ports_values):        
        """
        The generates the python script for the operator.

        Parameters
        ----------
        sql : list
            List of all the sql entries for the consumption element
        in_ports : list
            The input ports that have been generated and are needed to interact with
        out_ports_values : list
            The output port values. 

        Returns
        -------
        python_script : str
            The python script
        """
        indent = '    '
        python_script = []
        python_script.append('from hdbcli import dbapi')
        python_script.append('def on_input(' + ','.join(port['name'] for port in in_ports) + '):')
        
        # on_input function content (adding indent)
        python_script.append(indent + 'api.logger.debug("HANAML: Define connection context")')
        python_script.append(indent + 'address = api.config.hanaConnection["connectionProperties"]["host"]')
        python_script.append(indent + 'port =  api.config.hanaConnection["connectionProperties"]["port"]')
        python_script.append(indent + 'pwd =  api.config.hanaConnection["connectionProperties"]["password"]')
        python_script.append(indent + 'user =  api.config.hanaConnection["connectionProperties"]["user"]')
        python_script.append(indent + 'useTLS =  api.config.hanaConnection["connectionProperties"]["useTLS"]')
        python_script.append(indent + 'encrypt = "false"')
        python_script.append(indent + 'if useTLS:')
        python_script.append(indent + indent + 'encrypt = "true"')
        python_script.append(indent + 'hana_connection = dbapi.connect(address=address, port=port, user=user, password=pwd, encrypt=encrypt, sslValidateCertificate="false")')
        python_script.append(indent + 'api.logger.debug("HANAML: Generate SQL")')
        # Add sql statements
        python_script.append(indent + 'sql = """')
        python_script.append(indent + 'Do')
        python_script.append(indent + 'Begin')
        sql_str = StringUtils.flatten_string_array(sql, indent=indent)
        sql_str = self.config.data_source_mapping(sql_str)
        python_script.append(sql_str)
        python_script.append(indent + 'End')
        if in_ports:
            python_script.append(indent + '""".format(' + ','.join(port['name'] for port in in_ports) + ')')
        else:
            python_script.append(indent + '"""')
        python_script.append(indent + 'cursor = hana_connection.cursor()')
        python_script.append(indent + 'api.logger.debug("HANAML: Execute SQL")')
        python_script.append(indent + 'api.logger.debug("HANAML: " + sql)')
        python_script.append(indent + 'success = cursor.execute(sql)')
        python_script.append(indent + 'if success:')  
        
        for item in out_ports_values:
            python_script.append(indent + indent + 'api.send("' + item['port_name']+ '", \'' + item['dbobject_name'] + '\')')

        python_script.append(indent + 'api.logger.debug("HANAML: operator done")')  
        
        # Lastly based on if there are inports add api callback call or function call
        if in_ports:
            python_script.append('api.set_port_callback([' + '"'+ '", "'.join(port['name'] for port in in_ports) + '"' + '], on_input)')
        else:
            python_script.append('on_input()')

        return StringUtils.flatten_string_array(python_script)

    def _generate_port_connection(self, src_process, src_port, tgt_process, tgt_port):
        """
        Generates the actual graph json port connection structure

        Parameters
        ----------
        src_process : str
            The source operator from which the connection starts
        src_port : str
            The source oeprator port from which the connection starts
        tgt_process : str
            The target operator to where the connection ends
        tgt_port : str
            The target operator port to where the connection ends

        Returns
        -------
        connection : dict
            The connection structure.
        """
        tr_src_process = StringUtils.remove_special_characters(src_process).title()
        tr_tgt_process = StringUtils.remove_special_characters(tgt_process).title()
        connection = {
			"metadata": {},
			"src": {
				"port": src_port,
				"process": tr_src_process
			},
			"tgt": {
				"port": tgt_port,
				"process": tr_tgt_process
			}
		}
        return connection

    def _generate_port(self, name, port_type=None):
        """
        Generates the actual graph json port structure

        Parameters
        ----------
        name : str
            Name of the port to be used
        port_type : str
            Port type. For example string or message

        Returns
        -------
        port : dict
            The port structure.
        """
        if not port_type:
            port_type = 'string'
        port = {
                 'name': name,
                 'type': port_type
                }
        return port


    def _generate_operator(self, operator_comp, operator_id, operator_label, config, script, in_ports, out_ports, extensible):
        """
        Generates the actual graph json operator structure.

        Parameters
        ----------
        operator_comp : str
            Component of the operator as known by DataHub
        operator_id : str
            The operator id
        operator_label : str
            The operator label shown in the modeler on the operator
        config : dict
            Additional DataHub config that is required.
        script : str
            The python script which provides the operator functionality
        in_ports : list
            A list of additional in ports
        out_ports : list
            A list of additional out ports

        Returns
        -------
        operator : dict
            The operator structure.
        """
        tr_operator_id = StringUtils.remove_special_characters(operator_id).title()
        operator = {
			'component': operator_comp,
			'metadata': {
				'label': operator_label,
				'config': {},
			}
		}
        if config:
            operator['metadata']['config'] = config

        if extensible:
            operator['metadata']['extensible'] = True
        
        if script:
            operator['metadata']['config']['script'] = script

        if in_ports:
            operator['metadata']['additionalinports'] = in_ports

        if out_ports:
            operator['metadata']['additionaloutports'] = out_ports

        operator_object = {
            tr_operator_id : operator
        }
        return operator_object
    

    def _generate_graph(self, description, operators, connections):
        """
        Generates the actual graph json structure.

        Parameters
        ----------
        description : str
            Component of the operator as known by DataHub
        operators : list
            The list of already generated operators as to extend this with the generated operators. 
        connections : list
            The connections list for this graph which will be appended with the new connection

        Returns
        -------
        graph : dict
            The graph structure.
        """
        # Add operators
        processes = {}
        for operator in operators:
            processes.update( operator )
        
        graph = {
	        'description': '',
	        'processes': processes,
	        'groups': [],
	        'connections': connections,
	        'inports': {},
	        'outports': {},
	        'properties': {}
        }
        return graph


    # ----------------------------------------------------------
    # ML-Api
    # ----------------------------------------------------------
    def _generate_mlapi_model_operator(self, model_name, operator_comp, operator_id, operator_label):
        """
        Generates sap di ml api operators for model consumption or producing.

        Parameters
        ----------
        model_name : str
            Name of the model to use to store the model in ML API
        operator_comp : str
            Component of the operator as known by DataHub
        operator_id : str
            The operator id
        operator_label : str
            The operator label shown in the modeler on the operator

        Returns
        -------
        operators : list
            List of operators generated
        connections : list
            List of connections generated
        """
        config = {}
        operators = []
        connections = []
        if operator_comp == ConfigConstants.DATAHUB_MLAPI_OPERATOR_MDL_CONS_COMPONENT:
            # We need to add a constant generator which is a ml-api requirement
            config['content'] = '${ARTIFACT:MODEL:' + model_name + '}'
            constant_gen_id = 'const_' + operator_id
            constant_gen_label = 'Constant ' + operator_label
            constant_gen_op = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_CONSTANT_GENERATOR_COMPONENT, constant_gen_id, constant_gen_label, config, None, None, None, None)
            model_operator = self._generate_operator(operator_comp, operator_id, operator_label, None, None, None, None, None)
            # From constant generator op to ML-Api model consumer
            connections.append(self._generate_port_connection(constant_gen_id, ConfigConstants.DATAHUB_OPERATOR_CONSTANT_GENERATOR_OUT_PORT_STRING, operator_id, ConfigConstants.DATAHUB_OPERATOR_MDL_CONS_IN_PORT_STRING))
            operators.append(constant_gen_op)
            operators.append(model_operator)
        if operator_comp == ConfigConstants.DATAHUB_MLAPI_OPERATOR_MDL_PROD_COMPONENT:
            config['artifactKind'] = 'model'
            config['artifactName'] = model_name
            model_operator = self._generate_operator(operator_comp, operator_id, operator_label, config, None, None, None, None)
            operators.append(model_operator)
        return operators, connections
    

    # ----------------------------------------------------------
    # Rest Endpoint
    # ----------------------------------------------------------
    # Generate base path
    def _generate_rest_endpoint_operators(self, data_operator_id, data_outport_name, basePath):
        """
        Generates rest api related operators for exposing data external

        Parameters
        ----------
        data_operator_id : str
            The operator id
        data_outport_name : str
            The outport used to get data from which will need to be exposed
        basePath : str
            The base path for the url generated by DataHub for the rest endpoint

        Returns
        -------
        operators : list
            List of operators generated
        connections : list
            List of connections generated
        """
        operators = []
        connections = []
        # Build Operators
        # HANAML Api Python Operator
        result_client_op_id = data_operator_id + '_rescli'
        result_client_port_restrequest = self._generate_port(ConfigConstants.DATAHUB_OPERATOR_RESULT_IN_PORT_MESSAGE, port_type='message')
        result_client_port_restresponse = self._generate_port(ConfigConstants.DATAHUB_OPERATOR_RESULT_OUT_PORT_MESSAGE, port_type='message')
        result_client_port_datain = self._generate_port(ConfigConstants.DATAHUB_OPERATOR_RESULT_IN_PORT_STRING)
        result_client_inports = [result_client_port_restrequest, result_client_port_datain]
        result_client_outports = [result_client_port_restresponse]
        result_client_script = self._generate_result_client_script(result_client_port_datain['name'], result_client_port_restrequest['name'],result_client_port_restresponse['name'])
        result_client_operator = self._generate_operator(ConfigConstants.DATAHUB_HANAML_OPERATOR_COMPONENT,result_client_op_id,'Result Client', None, result_client_script, result_client_inports, result_client_outports, False)
        # OpenAPI Servflow Operator
        restapi_op_id = data_operator_id + '_restapi'
        restapi_op_config = {
            'basePath': basePath
        }
        restapi_operator = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_RESTAPI_COMPONENT,restapi_op_id,'Rest API', restapi_op_config, None, None, None, False)
        # Response Interceptor Operator
        resp_int_op_id = data_operator_id + '_respint'
        resp_int_operator = self._generate_operator(ConfigConstants.DATAHUB_OPERATOR_RESPONSE_INTER_COMPONENT,resp_int_op_id,'Response Interceptor', None, None, None, None, False)
        operators = [result_client_operator, restapi_operator, resp_int_operator]
        # Build Connections 
        # From Data --> Result Client
        data_to_result_client = self._generate_port_connection(data_operator_id, data_outport_name, result_client_op_id, result_client_port_datain['name'])
        # From Result Client --> Response intercepter
        result_client_to_resp_int = self._generate_port_connection(result_client_op_id, ConfigConstants.DATAHUB_OPERATOR_RESULT_OUT_PORT_MESSAGE, resp_int_op_id, ConfigConstants.DATAHUB_OPERATOR_RESPONSE_INTER_RESPONSE_IN_PORT_MESSAGE)
        # From Response intercepter --> Result Client
        resp_int_to_result_client = self._generate_port_connection(resp_int_op_id, ConfigConstants.DATAHUB_OPERATOR_RESPONSE_INTER_OUT_PORT_MESSAGE, result_client_op_id, ConfigConstants.DATAHUB_OPERATOR_RESULT_IN_PORT_MESSAGE)
        # From Rest API --> Response Interceptor
        restapi_to_resp_int = self._generate_port_connection(restapi_op_id, ConfigConstants.DATAHUB_OPERATOR_RESTAPI_OUT_PORT_MESSAGE, resp_int_op_id, ConfigConstants.DATAHUB_OPERATOR_RESPONSE_INTER_REQUEST_IN_PORT_MESSAGE)
        connections = [data_to_result_client, result_client_to_resp_int, resp_int_to_result_client, restapi_to_resp_int]
        return operators, connections
        
    # Restendpoint has url pattern: <protocol>://<host>:<port>/app/pipeline-modeler/openapi/service/<operator_config_basepath>
    # Best results to add / at the beginning and end of the basepath in the operator config. ie /hanaml/
    def _generate_result_client_script(self,in_port_data_name, in_port_request_name, out_port_name): 
        """
        Generates result client operator python script

        Parameters
        ----------
        in_port_data_name : str
            From which to retrieve the data (ie a HANA schema/table)
        in_port_request_name : str
            The in port to link the request of the data. This can be a rest endpoint operator
        out_port_name : str
            The out port to which the data is pushed.

        Returns
        -------
        python_script : str
            Generated python script 
        """       
        indent = '    '
        python_script = []
        python_script.append('import json')
        python_script.append('from hdbcli import dbapi')
        python_script.append('def on_request(msg):')
        python_script.append(indent + 'msg.attributes["openapi.header.content-type"] = "application/json"')
        python_script.append(indent + 'response = {}')
        python_script.append(indent + 'if "results" in globals():')
        python_script.append(indent + indent + 'response = {"results": results}')
        python_script.append(indent + 'else:')
        python_script.append(indent + indent + 'response = {"results": "Sorry no results yet. Please try again in a sec"}')
        python_script.append(indent + 'msg.body=json.dumps(response)')
        python_script.append(indent + 'api.send("' + out_port_name + '", msg)')
        python_script.append('def on_data(data):')
        python_script.append(indent + 'api.logger.debug("HANAML: Define connection context")')
        python_script.append(indent + 'address = api.config.hanaConnection["connectionProperties"]["host"]')
        python_script.append(indent + 'port =  api.config.hanaConnection["connectionProperties"]["port"]')
        python_script.append(indent + 'pwd =  api.config.hanaConnection["connectionProperties"]["password"]')
        python_script.append(indent + 'user =  api.config.hanaConnection["connectionProperties"]["user"]')
        python_script.append(indent + 'useTLS =  api.config.hanaConnection["connectionProperties"]["useTLS"]')
        python_script.append(indent + 'encrypt = "false"')
        python_script.append(indent + 'if useTLS:')
        python_script.append(indent + indent + 'encrypt = "true"')
        python_script.append(indent + 'hana_connection = dbapi.connect(address=address, port=port, user=user, password=pwd, encrypt=encrypt, sslValidateCertificate="false")')
        python_script.append(indent + 'api.logger.debug("HANAML: Generate SQL")')
        # Add sql statements
        python_script.append(indent + 'sql = "select * from {};".format(data)')
        python_script.append(indent + 'cursor = hana_connection.cursor()')
        python_script.append(indent + 'api.logger.debug("HANAML: Execute SQL")')
        python_script.append(indent + 'api.logger.debug("HANAML: " + sql)')
        python_script.append(indent + 'success = cursor.execute(sql)')
        python_script.append(indent + 'if success:')  
        python_script.append(indent + indent + 'global results')
        python_script.append(indent + indent + 'results = []')
        python_script.append(indent + indent + 'for row in cursor:')
        python_script.append(indent + indent + indent + 'results.append(str(row))')
        python_script.append(indent + 'api.logger.debug("HANAML: result operator done processing data")')  
        
        # Lastly based on if there are inports add api callback call or function call
        python_script.append('api.set_port_callback("' + in_port_request_name + '", on_request)')
        python_script.append('api.set_port_callback("' + in_port_data_name + '", on_data)')
        
        return StringUtils.flatten_string_array(python_script)

    def _get_rest_endpoint_path(self, postfix):
        """
        Helper function to generated rest endpoint paths

        Parameters
        ----------
        postfix : str
            Add additional component to the end of the path

        Returns
        -------
        endpoint_path : str
            Generated endpoint path
        """     
        return '/' + StringUtils.remove_special_characters(self.config.get_entry(ConfigConstants.CONFIG_KEY_PROJECT_NAME)).lower() + '-' + StringUtils.remove_special_characters(postfix).lower() + '/'


    # ----------------------------------------------------------
    # Generic helper methods
    # ----------------------------------------------------------
    def _generate_python_graph_finished_operator(self, operator_id, operator_label):
        """
        Helper function to generated graph terminator operator

        Parameters
        ----------
        operator_id : str
            The operator id
        operator_label : str
            The operator label shown in the modeler on the operator

        Returns
        -------
        operator : dict
            The graph json operator structure for the terminator operator
        """     
        # inports: metrics, artifact
        # outports: output
        metrics_inport = self._generate_port(ConfigConstants.DATAHUB_OPERATOR_TERMINATOR_IN_PORT_MESSAGE, port_type='message')
        artifact_inport = self._generate_port(ConfigConstants.DATAHUB_OPERATOR_TERMINATOR_IN_PORT_STRING)
        ouput_outport = self._generate_port(ConfigConstants.DATAHUB_OPERATOR_TERMINATOR_OUT_PORT_MESSAGE, port_type='message')
        script = self._generate_python_graph_finished_script([metrics_inport, artifact_inport], ouput_outport)
        operator = self._generate_operator(ConfigConstants.DATAHUB_HANAML_OPERATOR_COMPONENT, operator_id, operator_label, None, script, [metrics_inport, artifact_inport], [ouput_outport], None)
        return operator

    def _generate_python_graph_finished_script(self, in_ports, out_port):
        """
        Helper function to generated graph terminator python script

        Parameters
        ----------
        in_ports : list
            A list of additional in ports
        out_ports : list
            A list of additional out ports

        Returns
        -------
        python_script : str
            The generated python script
        """ 
        indent = '    '
        python_script = []
        python_script.append('def on_input(' + ','.join(port['name'] for port in in_ports) + '):')
        python_script.append(indent + 'api.send("' + out_port['name'] + '", api.Message("Operators have finished."))')
        python_script.append('api.set_port_callback(["' + '","'.join(port['name'] for port in in_ports) + '"], on_input)')
        return StringUtils.flatten_string_array(python_script)