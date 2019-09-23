"""
This module provides helper functionality for system prerequisites.
"""
import logging
import subprocess

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


if __name__ == '__main__':
    """
    Main functino for convenience to run the check functionality directly
    """
    pre_validator = PrerequisitesValidator()
    is_available, version = pre_validator.is_java_available()
    print("Result: ", is_available, version)
    is_available, version = pre_validator.is_cfcli_available()
    print("Result: ", is_available, version)
    is_available, version = pre_validator.is_cfcli_mta_available()
    print("Result: ", is_available, version)
    is_available, version = pre_validator.is_npm_available()
    print("Result: ", is_available, version)