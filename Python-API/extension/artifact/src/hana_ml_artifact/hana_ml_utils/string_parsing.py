"""
This module provides helper functionality for string related operations
"""
import logging
import pickle
import re

logger = logging.getLogger(__name__) #pylint: disable=invalid-name

class StringUtils(object):
    """
    This class provides helper functionality for string related operations
    """
    @staticmethod
    def findnth(string, substring, n):
        """
        Find the nth iteration of the substring and return the index

        Parameters
        ----------
        string : str
            The string to search in
        substring : str
            The substring to look for in the string
        enable_shell : boolean
            run through the shell

        Returns
        -------
        start : int
            The start index of the nth iteration
        """
        start = string.find(substring)
        while start >= 0 and n > 1:
            start = string.find(substring, start+len(substring))
            n -= 1
        return start
    
    @staticmethod
    def flatten_string_array(entries, seperator='\n', indent=''):
        """
        Flatten a string array/list to a single string

        Parameters
        ----------
        entries : list
            The string list
        seperator : str
            Line seperator between entries
        indent : str
            Indent in front of the entries

        Returns
        -------
        script_str : str
            The string result
        """
        script_str = ''
        for idx, entry in enumerate(entries):
            if entry:
                script_str += indent + entry
                if not idx == len(entries)-1:
                    script_str += seperator
        return script_str

    @staticmethod
    def multi_replace(string, replacements, ignore_case=False):
        """
        Replace multiple entries in a string at once

        Parameters
        ----------
        string : str
            The string to adjust
        replacements : dict
            The replacements
        ignore_case : boolean
            Should we care about case in the keys of the replacement dictionary

        Returns
        -------
        altered_string : str
            The altered string based on the replacements
        """
        if ignore_case:
            replacements = dict((pair[0].lower(), pair[1]) for pair in sorted(replacements.iteritems()))
        rep_sorted = sorted(replacements, key=lambda s: (len(s), s), reverse=True)
        rep_escaped = [re.escape(replacement) for replacement in rep_sorted]
        pattern = re.compile("|".join(rep_escaped), re.I if ignore_case else 0)
        return pattern.sub(lambda match: replacements[match.group(0)], string)

    @staticmethod
    def count_words(input_string, word):
        """
        Count the number of a specific word (or combination of charactes to be exact) 
        in a string

        Parameters
        ----------
        input_string : str
            The string to check
        word : str
            The combination of characters to search for

        Returns
        -------
        word_count : int
            The number of times the combination of characters has been found
        """
        return sum(1 for _ in re.finditer(r'\b%s\b' % re.escape(word), input_string))

    @staticmethod
    def count_char(input_string, char):
        """
        Count the number of a specific character
        in a string

        Parameters
        ----------
        input_string : str
            The string to check
        char : str
            The character

        Returns
        -------
        char_count : int
            The number of times the character has been found
        """
        return input_string.count(char)

    @staticmethod
    def remove_special_characters(input_string):
        """
        Remove special characters

        Parameters
        ----------
        input_string : str
            The string to check

        Returns
        -------
        cleaned_string : str
            The cleansed string
        """
        return ''.join(e for e in input_string if e.isalnum())
