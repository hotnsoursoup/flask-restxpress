from sqlalchemy import create_engine
from werkzeug.datastructures import ImmutableDict as iDict
from flask import g, current_app as app



# Validate the database dialect in the configuration file
try:
    dialect = app['config']['database']['dialect']
    if dialect not in ['mysql', 'mssql', 'postgresql', 'oracle', 'sqlite']:
        raise ("KeyError: Dialect must be one of: 'mysql', 'mssql', 'postgresql', 'oracle', 'sqlite'")
except KeyError as e:
    raise("Please check your configuration file for a valid database entry. Error: {e}".format(e))


allowed_connectors = {'sqlalchemy', 'odbc', 'dsn'}

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

class DBConfig:

    
    def __init__(self, config: dict):
        """Reads the database configuration to ensure the correct values are provided when providing
        connection information. 
    

        :param config: configuration dictionary for the database
        :type config: dict
        """        
        self._config = config
        self.conn = self.conn()
        
        self.connector = None
        self.driver = None
        self.connection_string = None
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

        
        if connector and connector in allowed_connectors:
            self.connector = self.conn.pop('connector')
        elif connector:
            raise ValueError(error_messages['invalid_connector'])
        # A connection string must always have a connector specified.
        if connection_string and connector:
            self.connection_string = connection_string
        elif connection_string:
            raise ValueError(error_messages['missing_connector_with_connection_string'])
        # Supported dialects: mysql, mssql, oracle, postgresql, sqlite
        if dialect:
            self.dialect = dialect
        elif not self.connection_string:
            # A connection string is required or a dialect must be provided.
            raise ValueError(error_messages['missing_dialect'])

        self.driver = self.conn.get('driver')
        self.args = self.conn.pop('args')
        self.params = self.conn.pop('params')
        self.function = self.conn.pop('function')
        


class DBConn(DBConfig):
    "A default class for database connections"
    def __init__(self, config=None, name=None):
        """Creates a database connection from a database configuration

        :param config: database configuration
        :type config: dict, list[dict]
        :param name: _description_, defaults to None
        :type name: _type_, optional
        """
        # Set using the db config assigned to app if none is provided.
        config = config if config else app.config['db']
        
        try:
            # Use the named database configuration, else default if assigned, else first db listed.
            if name: 
                config = config[name]
            elif self._default_db_name is not None:
                config = config['db'][self._default_db_name]['sqlalchemy']
            else:
                if isinstance(config, list) and len(config['db']) > 0:
                    config = config['db'][0]
                else:
                    next(iter(config.items()))
                    
            # Read and set connection parameters
            super().__init__(config)
            
            self.build_connection
                    
        except KeyError as e:
            raise("Please check your configuration file for a valid database entry.")
    
    def engine(self):
        "Create a SQLAlchemy engine connection."
        connection_string = self.get_connection_string()
        
        # Add in any connection options if provided
        engine_options = self.config.options if self.config.options else {}
        
        return create_engine(connection_string,  **engine_options)
    
    @property
    def _const(self, key):
        "constructor for sql alchemy connection strings"
        return ":" + str(self._conn.get(key)) if key in self._conn else ''

    ############ Need to check if having ":" without any following parameter will cause connection to fail
    @property
    def get_connection_string(self):
        port = self._const('port')
        password = self._const('password')
        # connection string 
        if self.connection_string:
            return self.connection_string
        return "{driver}://{username}{password}@{host}{port}/{database}".format(**self._conn)
        
       
    def get_default_dbconfig(self, configs: list) -> str:
        for config in configs:
            if config.get('default') == True:
                self._default_db_name = config['name']
    
    
    
def get_db():
    # Grabs the db object within the application context using g
    if 'db' not in g:
        g.db = DBConn()
        g.db.connect()
    return g.db


@app.teardown_appcontext
def teardown_db(exception=None):
    # Removes the db connection and closes it when the request is complete
    db = g.pop('db', None)
    if db is not None:
        db.conn.close()
  
        

