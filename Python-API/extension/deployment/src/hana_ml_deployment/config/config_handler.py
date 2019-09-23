"""
This module allows interaction with the configuration functionality.

It consists of two parts. 
1. The ConfigHandler which provides helper functions to support easy setting
and retrieving of configurations.
2. Config Constants for easy access to config values. 
"""
class ConfigHandler(object):
    """
    This class handles config related functions and helper functions.
    """
    def __init__(self):
        """
        Class for interacting with config data.
        """
        self.config = {}

    def add_entry(self, key, value):
        """
        Add data to the config under the provided key

        Parameters
        ----------
        key: str
            The key under which the value needs to be stored
        value: object
            Object that will be stored under the provided key
        """
        self.config[key] = value

    def get_entry(self, key):
        """
        Get data from the config using the provided key
    
        Parameters
        ----------
        key: str
            The key under which the value needs to be stored

        Returns
        -------
        value: object
            Object that will be stored under the provided key
        """
        return self.config[key]


class ConfigConstants(object):
    """
    This class provides a central repository of config constants they are defined here. This allows
    for easy adjustments without needed to change all the literals in the code. 
    """
    pass