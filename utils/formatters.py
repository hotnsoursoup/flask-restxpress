import re
import string
from glom import glom, T, Iter
from typing import Any, Union


def clean_data(data, lowercase=False):
    """ Cleans data based on type. Removes whitespaces, converts to lowercase if lowercase is True

    :param data: data to be cleaned and lowered if True
    :type data: dict, list, str
    :param lowercase: Lowercases nested structure, defaults to False
    :type lowercase: bool, optional
    :return: data
    :rtype: dict, list, str
    """    
    if isinstance(data, dict):
        return {clean_data(k, lowercase): clean_data(v, lowercase) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_data(item, lowercase) for item in data]
    elif isinstance(data, str):
        return re.sub(r"\s+", "", data.lower() if lowercase else data)
    else:
        return data

def capitalize(str: str):
    "Capitalizes the first character within a group"
    return re.sub(r"(^|[/!?]\s+)([a-z])", lambda m: m.group(1) + m.group(2).upper(), str)

def lowercase_nested_data(data):
    """ Lowercases all keys in nested dictionaries and lists 

    :param data: data to be lowercased
    :type data: str, dict, list
    :return: data
    :rtype: str, dict, list
    """    
    if isinstance(data, dict):
        return {k.lower(): lowercase_nested_data(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [lowercase_nested_data(item) for item in data]
    elif isinstance(data, str):
        return data.lower()
    else:
        return data
    
class SafeDict(dict):
    def __missing__(self, key):
        return '{' + key + '}'

def safe_format(format_string: str, **kwargs) -> str:
    """Format the string with available keys in the dictionary."""
    formatter = string.Formatter()
    safe_dict = SafeDict(**kwargs)
    return formatter.vformat(format_string, (), safe_dict)



def cleandict(dictionary: dict) -> dict:
    '''
    Removes None values from the dictionary.
    '''

    # if a list, clean each item in the list
    if isinstance(dictionary, list):
        return [cleandict(item) for item in dictionary]

    # if not a dictionary or a tuple, just return it
    if not isinstance(dictionary, dict):
        return dictionary

    return dict((key, cleandict(val))
                for key, val in dictionary.items() if val is not None)
    

# Custom filter function to remove empty values
def is_not_empty(value: Any) -> bool:
    return value not in ('', ' ', None, [], {})

# Function to recursively remove empty values
def remove_empty_values(data: Union[dict, list]) -> Union[dict, list]:
    spec = (
        T,
        Iter().filter(is_not_empty).map(
            lambda x: remove_empty_values(x) if isinstance(x, (dict, list)) else x
        ).all()
    )
    return glom(data, spec)

