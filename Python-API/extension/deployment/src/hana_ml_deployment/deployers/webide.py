"""
This module provides WebIDE related functionality
"""

import logging
import zipfile
import os

from ..hana_ml_utils import DirectoryHandler

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class WebIDEDeployer(object):
    """
    This class provides WebIDE deployer related functionality. 

    Currently the generation of a webide zip archive is provided.
    """
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
        self.directory_handler = DirectoryHandler()
        if not target_path:
            target_path = os.path.abspath(os.path.join(source_path, os.pardir))
        data_zip_file = zipfile.ZipFile(target_path + '/' + '{}.zip'.format(file_name), 'w')
        os.chdir(source_path)
        self.directory_handler.zip_directory(source_path, data_zip_file)
    