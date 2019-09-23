"""
This module provides DataHub related functionality.
"""
import logging
import os
import json
import requests

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class DHDeployer(object):
    """
    This class provides DataHub deployer related functionality. 

    It allows for deploying DataHub graphs through the REST API
    """
    def __init__(self, project,  host, port, useSSL, user, password, tenant='default', verify_ssl=False, vflow_local=False):
        """
        Initialize the DataHub Deployer.

        Parameters
        ----------
        project: str
            Project name as to create a folder in the DataHub repo under which the 
            graphs are stored. 
        host : str
            Host of the datahub tenant
        port : int
            Port of the datahub tenant
        useSSL : boolean
            Enable SSL or not.
        user : str
            Datahub user to deploy with.
        password : str
            Datahub user password to deploy with.
        tenant : str
            Datahub tenant name to deploy to.
        verify_ssl : boolean
            Verify the ssl certifcate
        vflow_local : boolean
            Use a local vflow engine for development purposes
        """
        self.datahub_restapi = DHRestApi(project, host, port, useSSL, user, password, tenant, verify_ssl, vflow_local)

    def deploy_graphs(self, path):
        """
        Deploy datahub graphs through the datahub rest api.

        Parameters
        ----------
        path : str
            Path to the graph jsons.
        """
        for file in os.listdir(path):
            if file.endswith(".json"):
                file_path = os.path.join(path, file)
                file_base = os.path.basename(file_path)
                file_name = os.path.splitext(file_base)[0]
                
                # Get json
                json_obj = self._get_graph_json_obj(file_path)
            
                # Create Graph    
                self.datahub_restapi.graph_update(file_name, json_obj)

    def _get_graph_json_obj(self, file_path):
        """
        Get graph as json object

        Parameters
        ----------
        file_path : str
            Path to the graph json.
        """
        graph_file = open(file_path, 'r')
        file_content = graph_file.read()
        return json.loads(file_content)

class DHRestApi(object):
    """
    This class represents the DataHub Rest API. 
    """
    GRAPH_URI = '/v1/repository/graphs/'
    NAME_PREFIX = 'hanaml.'
    def __init__(self, project, host, port, useSSL, user, password, tenant='default', verify_ssl=False, vflow_local=False):
        """
        Initialize the DataHub Rest API

        Parameters
        ----------
        project: str
            Project name as to create a folder in the DataHub repo under which the 
            graphs are stored. 
        host : str
            Host of the datahub tenant
        port : int
            Port of the datahub tenant
        useSSL : boolean
            Enable SSL or not.
        user : str
            Datahub user to deploy with.
        password : str
            Datahub user password to deploy with.
        tenant : str
            Datahub tenant name to deploy to.
        verify_ssl : boolean
            Verify the ssl certifcate
        vflow_local : boolean
            Use a local vflow engine for development purposes
        """
        protocol = "https://"
        if not useSSL:
            protocol = "http://"
        
        url = '{}{}:{}/app/pipeline-modeler/service'.format(protocol, host, port)
        if vflow_local:
            url = '{}{}:{}/service'.format(protocol, host, port)
        self.base_url = url
        self.verify_ssl = verify_ssl
        self.user = '{}\\{}'.format(tenant, user)
        self.password = password
        self.project = project

    def graph_update(self, name, json_object):
        """
        Update the graph in DataHub

        Parameters
        ----------
        name: str
            The name of the graph
        json_object : str
            The graph json object

        Returns
        -------
        success: boolean
            Whether the call was a success
        """
        if json_object:
            url = self._get_url(self.GRAPH_URI) + self.NAME_PREFIX + self.project + '.' + name
            header_params = {}
            header_params['Accept'] = 'application/json'
            header_params['Content-Type'] = 'application/json'
            response = requests.post(url, json=json_object, headers=header_params, auth=(self.user, self.password), verify=self.verify_ssl)
            if response.status_code == 201 or response.status_code == 200:
                logger.debug('Succesfully created graph')
            else:
                logger.error('Graph creation failed with status code {} and message {}'.format(response.status_code, response.text))
        return False

    def _get_url(self, function_part):
        """
        Get REST API url

        Parameters
        ----------
        function_part: str
            REST API function 

        Returns
        -------
        url: str
            REST API url
        """
        return self.base_url + function_part


