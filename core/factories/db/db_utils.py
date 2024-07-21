import re
from werkzeug.datastructures import ImmutableDict as iDict

def format_query(query, format_args=None):
    
    # Formats the query string with given arguments
    # Lowercase ensures consistency when formatting the args
    query = query.lower()
    
    if format_args == None:
        return query
    elif isinstance(format_args, list):
        return query.format(*format_args)
    elif isinstance(format_args, (dict, iDict)):
        return query.format(**format_args)        
    else:
        return query.format(format_args)


def trim_string(string, trim_carriage=True):
    # Trims query string carriage returns for better logging in 
    # case you have it imported using a formatter. You can also add 
    # back in carriage returns for keywords only as well.
    
    if trim_carriage:
        string = ' '.join(string.split())
    else:
        string = re.sub(' +', ' ', string)
    return string
