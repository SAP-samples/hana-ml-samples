"""
This module handles the generation of the files that represent the
artifacts.
"""
import json

from .filewriter_base import FileWriterBase

from ...config import ConfigConstants

class GraphWriter(FileWriterBase):
    """
    This class generates the graph json file
    """
    def generate(self, path, file_name, graph):
        """
        Generate the graph json file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        file_name : str
            The file name of the graph json file
        graph : dict
            The graph dictionary that needs to be written
        """
        self.write_file(path, file_name, graph)
    
    def write_file(self, path, file_name, graph):
        """
        Write the graph json file

        Parameters
        ----------
        path: str
            Physical location where to write the file
        file_name : str
            The file name of the graph json file
        graph : dict
            The graph dictionary that needs to be written
        """
        file_name = file_name + ConfigConstants.GRAPH_FILE_EXTENSION
        self.write_content(path, file_name, json.dumps(graph))