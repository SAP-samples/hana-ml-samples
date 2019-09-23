"""
This module is the entrypoint for deployment.
"""
import logging
import sys
import os
import shutil
import subprocess
from argparse import ArgumentParser

from .deployers import MTADeployer
from .deployers import DHDeployer
from .deployers import SDIDeployer
from .deployers import WebIDEDeployer

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

# TODO: add docu for npm install of npm sap repo
# TODO: node -v 1.12.3 npm 6.9.0 issue with node-gyp --> working node 10.12 / 10.16 only with hdi deploy

class Deployer(object):
    """
    This class contains the entrypoint for the deployment functionality.
    It provides the toplevel methods to deploy artifacts:

    * Cloud Foundry / MTAR. Mainly used for HANA HDI deployments
    * DataHub / SAP Data Intelligence Graphs through REST API
    * WebIDE zip generation for manual import into WebIDE. This is for the HANA artifacts only.
    """
    def deploy_mta(self, path, mta_builder, api, org, space, user, password):
        """
        Deploy mtar by building mtar from source and deploy through cf cli with mta plugin

        Parameters
        ----------
        path : str
            Path to the source that needs to be packaged into mtar and deployed.
        mta_builder : str
            Mtar builder jar location
        api : str
            Cloud Foundry api url
        org : str
            Cloud Foundry org to deploy to.
        space : str
            Cloud Foundry space to deploy to.
        user : str
            Cloud Foudry user to deploy with.
        password : str
            Cloud Foundry user password to deploy with.
        """
        mta_deployer = MTADeployer()
        mta_deployer.deploy_mta(path, mta_builder, api, org, space, user, password)
    
    def deploy_datahub(self, path, project, host, port, useSSL, user, password, 
                       tenant='default', verify_ssl=False, vflow_local=False):
        """
        Deploy datahubb graphs through the datahub rest api.

        Parameters
        ----------
        path : str
            Path to the graph jsons.
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
        dh_deployer = DHDeployer(project, host, port, useSSL, user, password,
                                 tenant, verify_ssl, vflow_local)
        dh_deployer.deploy_graphs(path)
    
    def generate_webide_package(self, source_path, target_path=None, file_name='WebIDE'):
        """
        Generates a zip archive that can be used to import the HANA artifacts into WebIDE.

        Parameters
        ----------
        source_path : str
            Path to the source that needs to be packaged into zip.
        target_path : str
            Where to place the generated zip archive
        file_name : str
            Zip archive file name
        """
        webide_deployer = WebIDEDeployer()
        webide_deployer.generate_webide_package(source_path, target_path, file_name)

    # Convenience methods 
    def deploy_cf(self, path, mta_builder, api, org, space, user, password):
        """
        Purely a convenience method. Currently the same as the deploy_mta.

        Parameters
        ----------
        path : str
            Path to the source that needs to be packaged into mtar and deployed.
        mta_builder : str
            Mtar builder jar location
        api : str
            Cloud Foundry api url
        org : str
            Cloud Foundry org to deploy to.
        space : str
            Cloud Foundry space to deploy to.
        user : str
            Cloud Foudry user to deploy with.
        password : str
            Cloud Foundry user password to deploy with.
        """
        self.deploy_mta(path, mta_builder, api, org, space, user, password)

    def deploy_hana(self, path, mta_builder, api, org, space, user, password):
        """
        Purely a convenience method. Currently the same as the deploy_mta.

        Parameters
        ----------
        path : str
            Path to the source that needs to be packaged into mtar and deployed.
        mta_builder : str
            Mtar builder jar location
        api : str
            Cloud Foundry api url
        org : str
            Cloud Foundry org to deploy to.
        space : str
            Cloud Foundry space to deploy to.
        user : str
            Cloud Foudry user to deploy with.
        password : str
            Cloud Foundry user password to deploy with.
        """
        self.deploy_mta(path, mta_builder, api, org, space, user, password)

    def deploy_sapdi(self, hana_path, sapdi_path, mta_builder, api, org, space,
                     cf_user, cf_password, host, port, useSSL, user, password,
                     tenant='default', verify_ssl=False, vflow_local=False):
        """
        Purely a convenience method. Combines the hana mta deployment and the graph deployment

        Parameters
        ----------
        hana_path : str
            Path to the hana source that needs to be packaged into mtar and deployed.
        sapdi_path : str
            Path to the datahub graph json that needs to be deployed through REST API
        mta_builder : str
            Mtar builder jar location
        api : str
            Cloud Foundry api url
        org : str
            Cloud Foundry org to deploy to.
        space : str
            Cloud Foundry space to deploy to.
        cf_user : str
            Cloud Foudry user to deploy with.
        cf_password : str
            Cloud Foundry user password to deploy with.
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
        self.deploy_mta(hana_path, mta_builder, api, org, space, cf_user, cf_password) 
        self.deploy_datahub(sapdi_path,host, port, useSSL, user, password, 
                            tenant, verify_ssl, vflow_local)
        

    
