"""
This module provides Multi-Target Application (MTA) related functionality.
"""
import logging
import os
import subprocess
import sys

# Additional Impprts for MTAGenerator
import zipfile
import shutil
from shutil import copyfile

from pathlib import Path

# TODO: Add shlex for proper subprocess.popen command list generation. Assures fringe cases on some OSs are handled as well. 

# Chosen to use cloudfoundry community python client to validate credentials / org / space as this is cleaner then handling this through subprocess and parsing the output stream. 
# However for the actual deploy subprocess needs to be used as this is a specific plugin and is not supported. Could be added to the python client but for now for 
# prototyping sake subprocess is used for this call and parsed accordingly.
from cloudfoundry_client.client import CloudFoundryClient
from cloudfoundry_client.operations.push.push import PushOperation

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# logging.basicConfig(stream=sys.stdout, format='',
                # level=logging.INFO, datefmt=None)
logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class PrerequisitesValidator(object):
    """
    This class provides file helper functionality.
    """
    # Depending on underlying logic being called the successfull output stream can be either stdout or stderr. This means
    # the stdout check only is not sufficient and requires additional checks on stderr. 
    def is_java_available(self):
        """
        Check if java is available

        Returns
        -------
        is_available : boolean
            Is java available
        version : str
            Java version
        """
        is_available = False
        # Java output is retrieved via the stderr stream of the subprocess. 
        # So check for error and handle accordingly
        result, error = self.call_subprocess(['java', '-version'])
        version = None
        if error:   # TODO: better handling to distinguish between actual error and a successful run received as error.
            is_available = True
            newline_index = result.index('\n')
            version = result[:newline_index]
        return is_available, version

    def is_cfcli_available(self):
        """
        Check if cloud foundry cli is available

        Returns
        -------
        is_available : boolean
            Is java available
        version : str
            Java version
        """
        is_available = False
        result, error = self.call_subprocess(['cf', '-v'])
        version = None
        if 'cf version' in result:
            is_available = True
            newline_index = result.index('\n') - 1
            version = result[11:newline_index]
        return is_available, version
    
    def is_cfcli_mta_available(self):
        """
        Check if cloud foundry cli mta plugin is available

        Returns
        -------
        is_available : boolean
            Is java available
        version : str
            Java version
        """
        is_available = False
        result, error = self.call_subprocess(['cf', 'deploy'])
        version = None
        if error == True:
            is_available = False
        elif error == False and 'not a registered command' in result:
            is_available = False
        elif error == False and 'Missing positional argument':
            is_available = True
            version = 'Unknown'
        return is_available, version

    def is_npm_available(self):
        """
        Check if npm (node package manager) is available

        Returns
        -------
        is_available : boolean
            Is java available
        version : str
            Java version
        """
        is_available = False
        result, error = self.call_subprocess(['npm', '-v'])
        version = None
        if not result is '':
            is_available = True
            version = result
        return is_available, version
    
    def call_subprocess(self, args, do_decode=True, enable_shell=False):
        """
        Interact with subprocess directly 

        Parameters
        ----------
        args : list
            The arguments used to launch the process. This may be a list or a string.
        do_decode : boolean
            decode the result
        enable_shell : boolean
            run through the shell

        Returns
        -------
        result : str
            Result of the call
        error : boolean
            Has an error occured
        """
        result = ''
        error = False
        try:
            proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=enable_shell)
            (out, err) = proc.communicate()
            if not err == b'':  # Check if we have a empty byte string
                result = err.decode('utf-8')
                error = True
            elif do_decode:
                result = out.decode('utf-8')
            else:
                result = out
        except:
            error=True
        return result, error


class MTADeployer(object):
    """
    This class provides MTA deployer related functionality. 

    It allows for deploying MTAR archives to cloud foundry.

    To be able to use this functionlity the following is required:

    * Cloud Foundry Client
    * MTA Plugin for Cloud Foundry CLI (only 2.0.7 at this stage)
    * node and NPM (Node Package Manager) installed
    * MTA Archive Builder
    """
    def __init__(self):
        """
        Initialize the MTA Deployer by validating the prerequisites
        """
        # Validate dependency to CF CLI
        self.validator = PrerequisitesValidator()
        self.is_cf_available, cf_version = self.validator.is_cfcli_available()
        self.is_cfmta_available, __ = self.validator.is_cfcli_mta_available()
        if not self.is_cf_available:
            logger.error('Cloud Foundry CLI is not available and required to perform deployments. Please download and install first before you continue. More info: https://tools.hana.ondemand.com/#cloud')
            exit(1)
        else:
            logger.info("Cloud Foundry CLI found with version: {}".format(cf_version))
            if not self.is_cfmta_available:
                logger.error('MTA Cloud Foundry CLI plugin not installed.  Please install, more info: https://tools.hana.ondemand.com/#cloud')
                exit(1)


    def deploy_mta(self, source_path, mta_builder, api, org, space, user, password):
        """
        Build mtar archive and deploy mtar.

        Parameters
        ----------
        source_path : str
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
        mtar_builder = MtarBuilder()
        mtar_file_path = mtar_builder.build_mtar(mta_builder, source_path, None)
        self.deploy_cf(mtar_file_path, api, org, space, user, password)
        
    def deploy_cf(self, mtar, api, org, space, user, password):
        """
        Deploy mtar archive

        Parameters
        ----------
        mtar : str
            Path to the mtar archive
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
        if not os.path.isabs(mtar):
            file_name = os.path.basename(mtar)
            mtar = os.path.join(os.getcwd(), file_name)

        if os.path.exists(mtar):
            # Verifying required data is provided
            if api == None or org == None or space == None or user == None or password == None:
                logger.error('Missing cl arguments. Minimal mtar, api, org, space, user, password required.')
                exit(1)

            # Setup cloud foundry client for validating org and space availability
            proxy = dict(http=os.environ.get('HTTP_PROXY', ''), https=os.environ.get('HTTPS_PROXY', ''))
            client = CloudFoundryClient(api, proxy=proxy, verify=False)

            # Login to api validating login credentials provided
            # CF Client does not properly allow for handling raised exception. It exits beforehand so try / except does not do much. Left now for reference. 
            try:
                client.init_with_user_credentials(user, password)
            except BaseException as ex:
                error_message = ''
                if type(ex).__name__ == 'OAuthError' or type(ex).__name__ == 'oauth2_client.credentials_manager.OAuthError':
                    # Credentials are not correct log accordingly
                    error_message = 'Provided username or password incorrect.'
                else:
                    error_message = 'Error {} occured while trying to authenticate with user {} against api {}.'.format(type(ex).__name__, user, api)
                logger.error(error_message)
                exit(1)

            # Get org and space guid
            org_guid = None
            space_guid = None
            for organization in client.v2.organizations.list(**{'name': [org]}):
                org_guid = organization['metadata']['guid']
                for org_space in organization.spaces(**{'name': [space]}): # perform a GET on spaces_url attribute
                    space_guid = org_space['metadata']['guid']

            if org_guid == None:
                logger.error('Provided orgnization {} is not available on the provided api url {}.'.format(org, api))
                exit(1)
            
            if space_guid == None:
                logger.error('Provided space {} is not available in the provided organization {}.'.format(space, org))
                exit(1)

            logger.info('Start deployment of: {}'.format(mtar))
            
            # Populate cli arguments for cf login
            cf_cli_args = ['cf', 'login']
            # Api
            cf_cli_args.extend(['-a', api])
            # Username
            cf_cli_args.extend(['-u', user])
            # Password
            cf_cli_args.extend(['-p', password])
            # Organization
            cf_cli_args.extend(['-o', org])
            # Space
            cf_cli_args.extend(['-s', space])

            # Call cf cli to login to correct api / org / space
            proc = subprocess.Popen(cf_cli_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            (out, err) = proc.communicate()
            result = out.decode('utf-8')
            if err == b'' and out.decode('utf-8').find('FAILED') == -1:
                # Login via cf cli is succesfull
                # Populate cli deploy arguments
                cf_cli_args = ['cf', 'deploy', mtar, '-f'] # @TODO: We are forcing deployment using the -f argument. Check if this can cause unwanted behaviour.
                proc = subprocess.Popen(cf_cli_args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

                process_failed = False
                err=None

                for line in proc.stdout: 
                    line_decoded = line.decode()
                    if process_failed and err == None:
                        err=line_decoded
                    if line_decoded.find('FAILED') > -1:
                        process_failed = True    
                    logger.info('CF_OUTPUT: {}'.format(line_decoded))

                if process_failed:
                    logger.error('Deployment not succesfull. Error: {}'.format(err))
                else:
                    logger.info('Deployment operation was succesfull. Please check output for other issues.')
            else:
                logger.error('Deployment not succesfull. Error: {}'.format(result))
                
            logger.info('MTAR Deployment process done')
        else:
            logger.error('Deployment could not be started as deployment file not found: {}'.format(mtar))

class MtarBuilder(object):
    """
    MTAR builder by using the mtar archive builder
    """
    JAR_ARGS_BUILDTARGET = '--build-target'
    JAR_ARGS_EXTENSION = '--extension'
    JAR_ARGS_SHOWDATADIR = '--show-data-dir'
    JAR_GENERIC_FILENAME = 'mtabuilder.jar'

    def __init__(self):
        """
        Initialize the MTAR builder.
        """
        # Validate dependency on mta builder jar
        self.validator = PrerequisitesValidator()
        self.is_java_available, java_version = self.validator.is_java_available()
        self.is_npm_available, npm_version = self.validator.is_npm_available()
        if not self.is_java_available:
            logger.error('JAVA is not available and is required. Please download and install first before you continue')
            exit(1)
        else:
            logger.info("JAVA found with version: {}".format(java_version))
        
        if not self.is_npm_available: # @TODO: Double check this requirement and see wether there is a way to generate mta project without npm requirement
            logger.error('NPM is not available and is required. Please download and install first before you continue')
            exit(1)
        else:
            logger.info("NPM found with version: {}".format(npm_version))


    def build_mtar(self, jar, mtadir, extension, buildtarget='CF', showdatadir=False):
        """
        Build the mtar using the mtar builder jar
        
        Parameters
        ----------
        jar : str
            MTAR Archive builder jar path
        mtadir : str
            Location of the source for the mtar
        extension : str
            Extending the mtar archive
        buildtarget : str
            Which build target to use.
        showdatadir : boolean
            Provide additional debugging functionality while building mtar
        
        Returns
        -------
        file_path: str
            The file path wher ethe mtar is created
        """

        if jar == None or mtadir == None:
            logger.error('Missing cl arguments. Minimal mtadir and jar required.')
            exit(1)
        elif not os.path.exists(jar):
            logger.error('Jar argument incorrect. {} does not exist'.format(jar))
            exit(1)
        elif not os.path.exists(mtadir):
            logger.error('Mtadir argument incorrect. {} does not exist'.format(mtadir))
            exit(1)
        else:
            logger.info('Starting MTA Generation')
            logger.info('MTA Builder Tool: {}, Content directory: {}'.format(jar, mtadir))        
        
        if not os.path.isabs(jar):
            jar = os.path.join(os.getcwd(), jar)
        
        # As the mta builder tool does not support defining the content location. The currrent os location needs to be changed
        # as the jar file assumes the current location is where the mta project dir is. 
        if os.path.isabs(mtadir):
            os.chdir(mtadir)
        else:
            os.chdir(os.path.join(os.getcwd(), mtadir))

        # Populate cli arguments for jar
        jar_args = ['java', '-jar']
        # Jar file
        jar_args.append(jar)
        # Build target
        jar_args.append(self.JAR_ARGS_BUILDTARGET + '=' + buildtarget)
        # Extension
        if not extension == None:
            jar_args.append(self.JAR_ARGS_EXTENSION + '=' + extension)
        # Show data dir
        if showdatadir:
           jar_args.append(self.JAR_ARGS_SHOWDATADIR)
        # Build commmand
        jar_args.append('build')

        subprocess.call(jar_args)

        logger.info('MTA Generation done')
        file_name = os.path.basename(mtadir) + '.mtar'
        file_path = os.path.join(mtadir, file_name)
        return file_path


    def zip_directory(self, path, zip_file):
        """
        Zip directory content

        Parameters
        ----------
        path : str
            Target location
        zip_file : zipfile.ZipFile
            zip file to which to add the content
        """
        for root, dirs, files in os.walk(path):
            for file in files:
                zip_file.write(os.path.join(root, file))
    
    def delete_directory_content(self, path):
        """
        Delete directory content

        Parameters
        ----------
        path : str
            Target location
        """
        for the_file in os.listdir(path):
            file_path = os.path.join(path, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path): shutil.rmtree(file_path)
            except Exception as e:
                print(e)
    
    def add_module_entry_to_manifest(self, name, manifest_file):
        """
        Helper function to create manifest file for mtar archive

        Parameters
        ----------
        name : str
            Name to use
        manifest_file : file
            The manifest file to write to
        """
        manifest_file.write('\n') 
        manifest_file.write('Name: ' + name + '/data.zip')
        manifest_file.write('\n') 
        manifest_file.write('MTA-Module: ' + name)
        manifest_file.write('\n') 
        manifest_file.write('Content-Type: application/zip')
        manifest_file.close()
