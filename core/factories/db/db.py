from typing import Dict, Any, Optional, Union, List
import pyodbc
import pandas as pd

from sqlalchemy import create_engine, text
from flask import g, current_app as app

from .db_utils import format_query


# Validate the database dialect in the configuration file
try:
    dialect = app.config['data']
    if dialect not in ['mysql', 'mssql', 'postgresql', 'oracle', 'sqlite']:
        raise ValueError("Dialect must be one of: 'mysql', 'mssql', 'postgresql', 'oracle', 'sqlite'")
except KeyError as e:
    raise("Please check your configuration file for a valid database entry. Error: {e}".format(e))


drivers = {
    "postgresql": "postgresql+psycopg2",
    "mysql": "mysql+pymysql",
    "sqlite": "sqlite",
    "oracle": "oracle+cx_oracle",
    "mssql": "mssql+pymssql"
    }


error_messages = {
    'missing connection': "Please check your configuration file. Missing connection information.",
    'missing_connector': "A connection string requires a connector: odbc, dsn, sqlalchemy",
    'missing_dialect': "Please check your configuration file. A valid dialect or connection string is required.",
    'invalid_connector': "Invalid connector specified. Only sqlalchemy, odbc, and dsn are allowed.",
    'missing_connector_with_connection_string': "A valid connector is required when using a connection string."
}


class BaseDB:

    
    def __init__(self, config: dict, name=None):
        """Reads the database configuration to ensure the correct values are provided when providing
        connection information. 
    

        :param config: configuration dictionary for the database
        :type config: dict
        :param name: dictionary key for the db config being looked up
        :type name: str
        """
        
        config = config if config else app.config['db']
        
        # Check if db is a key in the config, otherwise assume config is at root
        if 'db' in config:
            config = config['db']
        
        if name is not None:
            config = config[name]
        elif self._default_db_name is not None:
            config = config[self._default_db_name]
        else:
            # Grabs the first entry in case there are multiple.
            config = next(iter(config.items()))   

                 
        self._config = config
        self.conn = self.conn()
        
        self.connector = None
        self.driver = None
        self._connection_string = None
        self.dialect = None
        
        self.driver = None
        self.args = None
        self.params = None
        self.function = None
        self._default_db_name = None

    def conn(self):
        "Sets conn to the connection parameters from the db config. Shortens dictionary lookups"
        try:
            self.conn = self._config['connection']
        except KeyError:
            raise ValueError(error_messages['missing_connection'])
            

    def read_config(self):
        """Read the configuration and set to class. Remove keys not used for connection creation"""

        connection_string = self.conn.get('connection_string')
        connector = self.conn.get('connector')
        dialect = self.conn.get('dialect')

        
        allowed_connectors = {'sqlalchemy', 'odbc'}

        if connector and connector in allowed_connectors:
            self.connector = self.conn.pop('connector')
        elif connector:
            raise ValueError(error_messages['invalid_connector'])
        # A connection string must always have a connector specified.
        if connection_string and connector:
            self._connection_string = connection_string
        elif connection_string:
            raise ValueError(error_messages['missing_connector_with_connection_string'])
        # Supported dialects: mysql, mssql, oracle, postgresql, sqlite
        if dialect:
            self.dialect = dialect
        elif not self._connection_string:
            # A connection string is required or a dialect must be provided.
            raise ValueError(error_messages['missing_dialect'])

        self.driver = self.conn.get('driver')
        self.args = self.conn.pop('args')
        self.params = self.conn.pop('params')
        self.function = self.conn.pop('function')
    
           
    def get_default_dbconfig(self, configs: dict) -> str:
        for config in configs.items():
            if config.get('default') == True:
                self._default_db_name = config['name']

    def call_stored_procedure(
        self,
        procedure_name: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Union[Dict, List]:
        """Executes a stored procedure with the given parameters.
        
        :param procedure_name: The name of the stored procedure to call.
        :type procedure_name: str
        :param _params: A dictionary of parameter names and values (optional).
        :type _params: dict[str, Any] | None, optional

        :raises ValueError: If the database dialect is not supported.
        """

        # SQL templates based on dialect 
        sql_templates = {
            'postgresql': f"CALL {procedure_name}({', '.join([f':{key}' for key in params]) if params else ''})",
            'mysql': f"CALL {procedure_name}({', '.join([f':{key}' for key in params]) if params else ''})",
            'mssql': f"EXEC {procedure_name} {', '.join([f'@{key}=:{key}' for key in params]) if params else ''}",
            'oracle': f"BEGIN {procedure_name}({', '.join([f':{key}' for key in params]) if params else ''}); END;",
        }
        
        dialect = self.dialect

        # Ensure dialect is supported
        if dialect not in sql_templates:
            raise ValueError("Unsupported database dialect")
        
        # Select the appropriate version of procedure based on dialect
        sp_text = text(sql_templates[dialect])
        
        # Execute the stored procedure
        with self.engine.connect() as conn:
            results = conn.execute(sp_text)
        
        return self._process(results)
        
    def execute(self, query=None, params=None):
        "Handles query executions"
        
        sql = format_query(sql)
            
        with self.connect() as conn:
            
            result = conn.execute(text(sql))

    def _process(self, data):
        "Handles return type"
        results = [dict(row) for row in data]
        
        # Return one result as a dict
        if len(results) == 1 and self.output == 'dict':
            return results[0]
        else:
            # Return as a list of dicts
            return results
        
    def _page(self, data):
        "Handles pagination"
        
        # To be built
        
    def _marshal(self):
        "Response marshaling"
        
        # To be built
class SqAlchemyConn(BaseDB):
    "A default class for database connections using SqlAlchemy"
    def __init__(self, config=None, name=None):
        """Creates a database connection from a database configuration

        :param config: database configuration
        :type config: dict
        :param name: key name for the db being called. defaults to None
        :type name: str, optional
        """
        
        try:
            # Read and set connection parameters
            super().__init__(config, name=name)
            
            self._engine = self.engine
            
        except KeyError as e:
            raise("Please check your configuration file for a valid database entry.")
    
    @property
    def engine(self):
        "Create a SQLAlchemy engine connection."
        
        engine_options = self.config.options if self.config.options else {}
        
        return create_engine(self.connection_string,  **engine_options)
    
    @property
    def connection_string(self):
        if self._connection_string:
            return self._connection_string
        return "{driver}://{username}:{password}@{host}:{port}/{database}".format(**self._conn)
           
    def connect(self):
        return self.engine.connect()
    
            
    @property
    def commit(self):
        self.conn.commit()
        

class OdbcConn(BaseDB):
    "A default class for database connections using ODBC/DSN"
    def __init__(self, config=None, name=None):
        """Creates a database connection from a database configuration for ODBC connections

        :param config: database configuration
        :type config: dict
        :param name: key name for the db being called. defaults to None
        :type name: str,
        """
        # Set using the db config assigned to app if none is provided.
        
        
        try:
            # Read and set connection parameters
            super().__init__(config, name=name)
            
            self.build_connection
                    
        except KeyError as e:
            raise("Please check your configuration file for a valid database entry.")
    
     
    def connect(self):
    # Connects to the DB via odbc using the connection string.
    # We return the object and assign itself the connection to provide
    # 2 methods to access the connection
        self.conn = pyodbc.connect(self.dbstring)
        return self.conn

    @property
    def connection_string(self):
        
        driver = self.conn.get('driver')
        connection_str = [f"DRIVER={{{driver}}}"]
        
        try:
            for key, value in self.conn.items():
                if isinstance(value, bool):
                    value = 'yes' if value else 'no'
                    connection_str.append(f"{key.upper()}={value}")
        except KeyError as e:
            raise("Please check your configuration file for a valid database entry.")
        
        return connection_str
    
    def dsn_connection_string(self):
        # To be built
        ""
        
    

_dbclass = {
    "odbc": OdbcConn,
    "sqlalchemy": SqAlchemyConn
}

def get_db(name=None):
    # Grabs the db object within the application context using g
    if 'db' not in g:
        g.db = 
        g.db.connect()
    return g.db


@app.teardown_appcontext
def teardown_db(exception=None):
    # Removes the db connection and closes it when the request is complete
    db = g.pop('db', None)
    if db is not None:
        db.conn.close()
  
        

