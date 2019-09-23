"""
This module provides file system helper functionality.
"""
import os
import zipfile
import fileinput
import shutil

class FileHandler(object):
    """
    This class provides file helper functionality.
    """
    def __init__(self):
        """
        This class provides file helper functionality.
        """
        self.folder_handler = DirectoryHandler()

    def write_text_file(self, path, file_name, content):
        """
        Write content to a text file

        Parameters
        ----------
        path : str
            Path of where the file needs to be written
        file_name : str
            The file name
        content : str
            The content of the file
        """
        if self.folder_handler.validate_path(path):
            file_location = os.path.join(path, file_name)
            file = open(file_location, 'w+')
            file.write(str(content))
            file.close()
            return True
        return False
       

class DirectoryHandler(object):
    """
    This class provides directory helper functionality.
    """
    def copy_directory(self, from_path, to_path):
        """
        Copy complete directory recursively

        Parameters
        ----------
        from_path : str
            Source location
        to_path : str
            Target location
        """
        shutil.copytree(from_path, to_path)

    def create_directory(self, path):
        """
        Create deep directory structure

        Parameters
        ----------
        path : str
            Root target location
        """
        os.makedirs(path)

    def validate_path(self, path):
        """
        Generically validate path existence

        Parameters
        ----------
        path : str
            Target location
        """
        if not os.path.exists(path):
            raise IOError('Path {} does not exist'.format(path))
        return True

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
        if self.validate_path(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    zip_file.write(os.path.join(root, file))