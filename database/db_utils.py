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


def is_stored_procedure(query: str) -> bool:
    query = query.strip().lower()

    # Patterns to detect stored procedure calls
    patterns = [
        r"^exec\s",   # SQL Server, Sybase
        r"^execute\s", # SQL Server, Sybase (alternative)
        r"^call\s",   # MySQL, PostgreSQL, Oracle
        r"^begin\s",  # PL/SQL block in Oracle
        r"^declare\s" # PL/SQL anonymous block
    ]

    for pattern in patterns:
        if re.match(pattern, query):
            return True
    return False

def has_sorting(sql: str) -> bool:
    # Remove any subqueries or content within parentheses
    sql = re.sub(r'\([^()]*\)', '', sql)
    
    # Check if 'ORDER BY' exists outside of subqueries
    order_by_pattern = re.compile(r'\bORDER\s+BY\b', re.IGNORECASE)
    
    return bool(order_by_pattern.search(sql))