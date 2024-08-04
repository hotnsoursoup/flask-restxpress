  
def connect(self):
    # Connects using the DB via PYODBC using the connection string
    # We return the object and assign itself the connection to provide
    # 2 methods to access the connection
    self.conn = pyodbc.connect(self.dbstring)
    return self.conn
@property
def odbc_connection_string(self):
    
    connection_string = self.base_odbc_string
    try:
        for key, value in self.conn.items():
            value = config.get(key)
            if value:
                connection_string += f"{key}={value};"
    except KeyError as e:
        raise("Please check your configuration file for a valid database entry.")
    
    return connection_string
    
@property
def base_dsn_string(self):
    return "DSN=%s;UID=%s;PWD=%s;"




import pyodbc
from typing import Optional, Dict, Union

def create_odbc_connection(config: Dict[str, Union[str, int, bool, Dict[str, Optional[Union[str, int, bool]]]]]) -> pyodbc.Connection:
    """
    Create an ODBC connection using the provided configuration.

    Parameters:
        config (Dict[str, Union[str, int, bool, Dict[str, Optional[Union[str, int, bool]]]]]): 
            A dictionary of the ODBC configuration, including options.

    Returns:
        pyodbc.Connection: An ODBC connection object.
    """
    # Extract the driver
    driver = config.get('driver')
    if not driver:
        raise ValueError("Driver is required for ODBC connection")

    # Start building the connection string
    connection_str = [f"DRIVER={{{driver}}}"]

    # Handle optional connection settings
    connection_settings = config.get('options', {})
    for key, value in connection_settings.items():
        if isinstance(value, bool):
            value = 'yes' if value else 'no'
        connection_str.append(f"{key.upper()}={value}")

    # Join all parts of the connection string
    connection_str = ';'.join(connection_str)

    # Create the ODBC connection
    return pyodbc.connect(connection_str)

# Example usage
config = {
    'driver': 'MySQL ODBC 8.0 Driver',
    'options': {
        'server': 'localhost',
        'port': 3306,
        'database': 'mydatabase',
        'uid': 'username',
        'pwd': 'password',
        'trusted_connection': False,
        'app': 'MyApp',
        'connection_timeout': 30,
        'encrypt': True,
        'trust_server_certificate': False,
        'charset': 'utf8',
        'read_only': False,
        'autocommit': True,
        'pooling': True
    }
}

connection = create_odbc_connection(config)
