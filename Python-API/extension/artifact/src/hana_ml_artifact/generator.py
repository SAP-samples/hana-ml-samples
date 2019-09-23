"""
This module is the entrypoint for artifact generation.
"""
# TODO: Exception handling and message generation check
# TODO: Improve temp tale generation with more human readable names
import logging
import os
import pandas as pd

from .generators import HanaGenerator
from .generators import HanaSDAGenerator
from .generators import DataHubGenerator
from .generators import CloudFoundryGenerator
from .generators import AMDPGenerator
from .config import ConfigHandler
from .config import ConfigConstants
from .sql_processor import SqlProcessor
from .hana_ml_utils import DirectoryHandler
from .hana_ml_utils import StringUtils

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class Generator(object):
    """
    This class contains the entrypoint for artifact generation.
    It provides the toplevel methods to generate the following target artifacts:

    * HANA HDI Container artifacts
    * DataHub / SAP Data Intelligence Graphs
    * Abap AMDP. This is experimental.
    * Cloud Foundry Python Application. This is on the backlog and not yet implemented

    The class provides support for several concepts:

    Layered Generation (base and consumption layer)
    -----------------------------------------------
    The artifact package generation has the concept of a 2 layered approach:
    1. The low level layer, aka base layer, which is the wrapper around the PAL function
    and holds the defined parameters that go with the fucntion call. Input (data/model)
    and output is part of the interface of the base layer procedures. The artifacts
    of this layer is always HANA procedures.
    2. The top level layer, aka consumption layer, which consumes the base layer procedures
    and provides the correct input and uses the output. The consumption layer can be different
    artifacts. For example a DataHub graph can act as consumption layer by using a python
    operator to call the base layer procedures in HANA. The consumption layer can also be a
    seperate HANA procudure consuming the base layer procedures.

    In essence the base layer procedures and related HDI container artifacts are always
    genreated and depending on the users use case the respective consumption layer will
    be generated. Currently 3 consumption layer targets are supported:

    * HANA
    * DataHub
    * AMDP (ABAP) Experimental

    Data source mapping
    -------------------
    Another concept which this class provides is the notion of data source mapping. This allows
    for remapping the data source. This can be helpfull in case you want to generate based on
    1 experimentation with HANA ML multiple hdi containers for different target HANA systems where
    the source data is stored in different tables. Please keep in mind that this functionality
    is assuming the same datatype structure in the different systems.
    """

    def __init__(self, project_name, version, grant_service, connection_context, outputdir, #pylint: disable=too-many-arguments
                 generation_merge_type=ConfigConstants.GENERATION_MERGE_NONE,
                 generation_group_type=ConfigConstants.GENERATION_GROUP_FUNCTIONAL,
                 sda_grant_service=None, remote_source=''):
        """
        Entry class for artifact generation.

        Parameters
        ----------
        project_name : str
            The project name which will be used across the artifact generation such as folder that
            is created where the generated artifacts are placed.
        version : str
            The version to add to distinguish between multiple runs of the same project.
        grant_service: str
            The Cloud Foundry grant service that is used to grant the HDI container tech user the
            proper access during the deployment
        connection_context: object
            The HANA ML connection context object used. This holds the sql trace object required
            to generate the artifacts
        outputdir: str
            The location where the artifacts need to placed after generation.
        generation_merge_type: int
            Merge type is which operations should be merged together. There are at this stage
            only 2 options
            1: GENERATION_MERGE_NONE: All operations are generated seperately (ie. individual
            procedures in HANA)
            2: GENERATION_MERGE_PARTITION: A partition operation is merged into the respective
            related operation
            and generated as 1 (ie prodedure in HANA).
        generation_group_type: int
            11: GENERATION_GROUP_NONE # No grouping is applied. This means that solution specific
            implementation will define how to deal with this
            12: GENERATION_GROUP_FUNCTIONAL # Grouping is based on functional grouping. Meaning
            that logical related elements such as partiion / fit / and related score will be
            put together
        sda_grant_service: str
            When generating sda artifacts which grant service can be used to access the right
            grants.
        remote_source: str
            When generating sda artifacts what is the name of the remote source to be used.
        """
        self.directory_handler = DirectoryHandler()
        self.config = ConfigHandler()
        self._init_config(project_name,
                          version,
                          grant_service,
                          outputdir,
                          generation_merge_type,
                          generation_group_type,
                          sda_grant_service,
                          remote_source)
        sql_processor = SqlProcessor(self.config)
        sql_processor.parse_sql_trace(connection_context)

    def generate_amdp(self):
        """
        (Experimental) Generate ABAP ADMP classses.
        """
        amdp_generator = AMDPGenerator(self.config)
        amdp_generator.generate()

    def generate_hana(self, base_layer=True,
                      consumption_layer=True,
                      sda_data_source_mapping_only=True):
        """
        Generate HANA hdi artifacts.

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
        hana_generator = HanaGenerator(self.config)
        hana_generator.generate_artifacts(base_layer,
                                                 consumption_layer,
                                                 sda_data_source_mapping_only)

    def generate_hana_sda(self, model_only=True, sda_data_source_mapping_only=False):
        """
        Generate HANA hdi artifacts for the SDA scenario. Be aware that 2 containers 
        with there respective artifacts are created. The first is the same which
        includes both base and consumption layer artifacts. The second is the SDA
        container which loads and uses data out of the first container.

        Parameters
        ----------
        model_only: boolean
            In the sda case we are only interested in transferring the model using SDA.
            This forces the HANA artifact generation to cater only for this scenario.
        sda_data_source_mapping_only: boolean
            In case data source mapping is provided you can forrce to only do this for the
            sda hdi container
        """
        hana_sda_generator = HanaSDAGenerator(self.config)
        # Create hana objects. We will re-use consumption layer when doing the remote calls
        self.generate_hana(base_layer=True,
                           consumption_layer=True,
                           sda_data_source_mapping_only=sda_data_source_mapping_only)
        hana_sda_generator.generate_artifacts(model_only)

    def generate_sapdi(self, generate_hana_artifacts=True, include_rest_endpoint=False):
        """
        Generate SAP Data Intelligence Artifacts. This consists of both HANA as DataHub
        artifacts

        Parameters
        ----------
        generate_hana_artifacts: boolean
            Whether to generate the HANA artifacts or if only the graph should be generated
        include_rest_endpoint: boolean
            Include a rest endpoint. Normal SAP DI usage needs a rest endpoint.
        """
        self._generate_datahub(generate_hana_artifacts=generate_hana_artifacts,
                               include_rest_endpoint=include_rest_endpoint,
                               include_ml_operators=True)

    def generate_datahub(self, generate_hana_artifacts=True, include_rest_endpoint=False):
        """
        Generate DataHub Artifacts. This consists of both HANA as DataHub
        artifacts

        Parameters
        ----------
        generate_hana_artifacts: boolean
            Whether to generate the HANA artifacts or if only the graph should be generated
        include_rest_endpoint: boolean
            Include a rest endpoint. Normal SAP DI usage needs a rest endpoint.
        """
        self._generate_datahub(generate_hana_artifacts=generate_hana_artifacts,
                               include_rest_endpoint=include_rest_endpoint,
                               include_ml_operators=False)

    def generate_cf(self):
        """
        Not Implemented - Generate Cloud Foundry Artifacts.
        """
        cloudfoundry_generator = CloudFoundryGenerator(self.config)
        # Create base hana objects. No need for a hana consumption layer as
        # python program will act as the consumption layer
        self.generate_hana(base_layer=True, consumption_layer=False)
        cloudfoundry_generator.generate_artifacts()

    # ---------------------------------------------------------------------------------------------
    #  Convenience Methods
    # ---------------------------------------------------------------------------------------------
    def clean_outputdir(self):
        """
        Clean the output dir where artifacts will be generated.
        """
        path = self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_DIR)
        if os.path.exists(path):
            self.directory_handler.delete_directory_content(path)
            os.rmdir(path)

    def get_output_path_hana(self):
        """
        Get the output path of the hana artifacts

        Returns
        -------
        hana_output_path: str
            Returns the physical location where the hana artifacts are stored.
        """
        return self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_HANA)

    def get_output_path_hana_sda(self):
        """
        Get the output path of the sda related hana artifacts

        Returns
        -------
        hana_sda_output_path: str
            Returns the physical location where the hana artifacts are stored.
        """
        return self.config.get_entry(ConfigConstants.CONFIG_KEY_SDA_OUTPUT_PATH_HANA)

    def get_output_path_sapdi(self):
        """
        Get the output path of the datahub related artifacts

        Returns
        -------
        sapdi_output_path: str
            Returns the physical location where the sapdi artifacts are stored.
        """
        return self.get_output_path_datahub()

    def get_output_path_datahub(self):
        """
        Get the output path of the datahub related artifacts

        Returns
        -------
        datahub_output_path: str
            Returns the physical location where the datahub artifacts are stored.
        """
        return self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_DATAHUB)

    def get_output_path_cf(self):
        """
        Get the output path of the cloud foundry python app related artifacts

        Returns
        -------
        cf_output_path: str
            Returns the physical location where the cf artifacts are stored.
        """
        return self.config.get_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH_CF)

    def show_hana_data_source_mapping(self):
        """
        Prints out the data source mapping currently configured.

        Returns
        -------
        data_source_mapping: dataframe
            Returns the datasource mapping as a pandas dataframe for formatted display
            Mainly usefull in jupyter notebook scenario.
        """
        data = self.get_hana_data_source_mapping()
        return pd.DataFrame.from_dict(data, orient='index', columns=["Data Source Map To:"])

    def get_hana_data_source_mapping(self):
        """
        Get the the current data source mapping. Which can be used to adjust mapping.

        Returns
        -------
        data_source_mapping: dict
            Returns the datasource mapping as a dictionary
        """
        return self.config.get_entry(ConfigConstants.CONFIG_KEY_DATA_SOURCE_MAPPING)

    def set_hana_data_source_mapping(self, data_source_mapping):
        """
        Set the data data source mapping.

        Parameters
        ----------
        data_source_mapping: dict
            dictionary with data source mapping.
        """
        self.config.add_entry(ConfigConstants.CONFIG_KEY_DATA_SOURCE_MAPPING,
                              data_source_mapping)

    def _generate_datahub(self,
                          generate_hana_artifacts=True,
                          include_rest_endpoint=False,
                          include_ml_operators=False):
        """
        Method to start the generation of the graph json for DataHub.

        Parameters
        ----------
        generate_hana_artifacts: boolean
            Whether to generate the HANA artifacts or if only the graph should be generated
        include_rest_endpoint: boolean
            Include a rest endpoint. Normal SAP DI usage needs a rest endpoint.
        include_ml_operators: boolean
            Include ML operators for the SAP DI scenario
        """
        datahub_generator = DataHubGenerator(self.config)
        # Create base hana objects. No need for a hana consumption layer as sapdi will act
        # as the consumption layer
        if generate_hana_artifacts:
            self.generate_hana(base_layer=True, consumption_layer=False)
        datahub_generator.generate_artifacts(include_rest_endpoint, include_ml_operators)

    def _init_config(self, #pylint: disable=too-many-arguments
                     project_name,
                     version,
                     grant_service,
                     outputdir,
                     generation_merge_type,
                     generation_group_type,
                     sda_grant_service,
                     remote_source):
        """
        Method to initiate the configuration.

        Parameters
        ----------
        project_name : str
            The project name which will be used across the artifact generation such as folder that
            is created where the generated artifacts are placed.
        version : str
            The version to add to distinguish between multiple runs of the same project.
        grant_service : str
            The Cloud Foundry grant service that is used to grant the HDI container tech user the
            proper access during the deployment
        outputdir : str
            The location where the artifacts need to placed after generation.
        generation_merge_type : int
            Merge type is which operations should be merged together. There are at this stage
            only 2 options
            1: GENERATION_MERGE_NONE: All operations are generated seperately (ie. individual
            procedures in HANA)
            2: GENERATION_MERGE_PARTITION: A partition operation is merged into the respective
            related operation
            and generated as 1 (ie prodedure in HANA).
        generation_group_type : int
            11: GENERATION_GROUP_NONE # No grouping is applied. This means that solution specific
            implementation will define how to deal with this
            12: GENERATION_GROUP_FUNCTIONAL # Grouping is based on functional grouping. Meaning
            that logical related elements such as partiion / fit / and related score will be
            put together
        sda_grant_service:  str
            When generating sda artifacts which grant service can be used to access the right
            grants.
        remote_source : str
            When generating sda artifacts what is the name of the remote source to be used.
        """
        # Remove improper characters
        project_name = StringUtils.remove_special_characters(project_name)
        module_name = project_name
        app_id = module_name
        schema = '"'+(module_name + '_SCHEMA').upper()+'"'
        # This is the root folder in the outputdir where the artifacts will be generated.
        output_path = os.path.join(outputdir, project_name)

         # Populate config
        self.config.add_entry(ConfigConstants.CONFIG_KEY_OUTPUT_PATH, output_path)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_PROJECT_NAME, project_name)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_VERSION, version)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_GRANT_SERVICE, grant_service)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_GRANT_SERVICE, sda_grant_service)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SDA_REMOTE_SOURCE, remote_source)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_MERGE_STRATEGY, generation_merge_type)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_GROUP_STRATEGY, generation_group_type)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_OUTPUT_DIR, outputdir)


        self.config.add_entry(ConfigConstants.CONFIG_KEY_MODULE_NAME, module_name)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_CDS_CONTEXT, 'output')
        self.config.add_entry(ConfigConstants.CONFIG_KEY_APPID, app_id)

        self.config.add_entry(ConfigConstants.CONFIG_KEY_SCHEMA, schema)
        self.config.add_entry(ConfigConstants.CONFIG_KEY_SQL_PROCESSED, {})

        self.config.add_entry(ConfigConstants.CONFIG_KEY_DATA_SOURCE_MAPPING, {})
