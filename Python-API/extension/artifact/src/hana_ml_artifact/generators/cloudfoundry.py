"""
This module handles generation of all Cloud Foundry related artifacts based on the provided
consumption layer elements. Currently this has not yet been implemented
"""
import os 

from ..config import ConfigConstants
from ..hana_ml_utils import DirectoryHandler
from ..hana_ml_utils import StringUtils

from ..sql_processor import SqlProcessor

class CloudFoundryGenerator(object):
    def __init__(self, config):
        self.config = config
        self._extend_config()

    def generate_artifacts(self):
        return ''

    def _extend_config(self):
        pass


class CloudFoundryConsumptionProcessor(object):
    def __init__(self, config):
        self.config = config

    def generate(self, path):
        pass