from glom import glom, T, Iter
from typing import Any, Union

def get_dict_value(key: str, dictionary: dict, default_value: str = ''):
    "Retrieve dictionary values with the existence of tuples as keys"
    if dictionary and isinstance(dictionary, dict):
        if any(isinstance(k, tuple) for k in dictionary):
            for k, v in dictionary.items():
                if key in k:
                    return v if v else default_value
        else:       
            return dictionary.get(key, default_value)
    else:
        return default_value

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
