  
def connect(self):
    # Connects using the DB via PYODBC using the connection string
    # We return the object and assign itself the connection to provide
    # 2 methods to access the connection
    self.conn = pyodbc.connect(self.dbstring)
    return self.conn
@property
def odbc_connection_string(self):
    
    connection_string = self.base_odbc_string
    
    fields = {
        'host': 
        'port': 'Port',
        'username': 'UID',
        'password': 'PWD',
        'host':
    }
    
    try:
        for key, field_name in fields.items():
            value = dbconfig.get(key)
            if value:
                connection_string += f"{field_name}={value};"
    except KeyError as e:
        raise("Please check your configuration file for a valid database entry.")
    
    return connection_string
    
@property
def base_dsn_string(self):
    return "DSN=%s;UID=%s;PWD=%s;"

def execute(self, query=None, args=None, procedure=None):
    # Used for delete, update, insert, etc.
    try:
        with self.conn as conn:
            
            if procedure:
                # Get the procedure sql string
                query = build_procedure_sql_args(procedure, args)
            else:
                # Format the query string
                query = format_query(query, args)
        
            if g.request.method == 'GET':
                # Results are returned as a list of dictionaries
                df = pd.read_sql(query, conn)
                
                if not df.empty:
                    result = df.to_dict(orient='records')

                    result = lowercase_all(result)
                    
                    return result[0] if len(result) == 1 else result
                
                return None
            else:
                with conn.cursor() as cursor:
                    # This will execute the write command, but it is important
                    # that you commit the changes to the database with validation
                    # in the custom function
                    cursor.execute(query)
                
                
    except pyodbc.Error as e:
        msg = e.args[1]
        logger.error(msg)
        logger.error(query)

def commit(self):
    return self.conn.commit()
def log(query):
    logger.debug(f'{trim_string(string=query)}')


logger = logging.getLogger('error')


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
