import pyodbc
import warnings
from abc import ABC
from abc import abstractmethod
from typing import Dict, Any, Optional, Union, List, Tuple, Callable

from flask import current_app as app
from flask import g
from models.db_model import validate_db_config
from .db_utils import is_stored_procedure
from .db_utils import has_sorting
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker


# Supported database drivers
drivers = {
    "postgresql": "postgresql+psycopg2",
    "mysql": "mysql+pymysql",
    "sqlite": "sqlite",
    "oracle": "oracle+cx_oracle",
    "mssql": "mssql+pymssql"
}


error_messages = {
    "missing_params": "Please check your configuration file. Missing connection information.",
    "missing_type": "A connection string requires a type: odbc, dsn, sqlalchemy",
    "missing_dialect": "Please check your configuration file. A valid dialect or connection string is required.",
    "invalid_connector": "Invalid connector specified. Only sqlalchemy, odbc, and dsn are allowed.",
    "missing_connector_with_uri": "A valid connector is required when using a connection string.",
    "missing_db_config": "Please check your configuration file for a valid database entry.",
    "named_db_not_found": "The named database configuration was not found in the configuration file.",
    "operational_error:": "Operational error: Failed to connect to the database. Details: {}",
    "unsupported_dialect": "The database dialect is not supported. Please refer to documentation for supported dialects.",
    "key_error": "Please check your configuration file for a valid database entry."
}


def set_defaults(config: dict):
    # Set the default db name for the class if one is desginated.
    default_db_name = get_default_db(config)
    BaseDB.default_db_name = default_db_name
    BaseDB.default_type = config[default_db_name].get('type')
    

def get_default_db(self, config: dict) -> Union[bool, str]:
    """
    Returns the default database configuration from the 
    configuration file. We rely on the config model validation
    to ensure there is only one default db if there is one provided.
    
    
    :param config: configuration dictionary for the database
    :type config: dict
    """
    
    for db_name, config in config.items():
        if config.get('default') == True:
            self.default_db_name = db_name
    return next(iter(config.items()))

class BaseDB(ABC):
    "We set the default db to be called if a named db is not called."
    default_db_name = None
    
    "Default references if one is not provided."
    _orm = False
    _auto_page = False
    
    def __init__(self, 
        config: dict, name=None,
        methods: Dict[str, Callable] = None
        ):
        """
        Base database object. Provides basic structure and functionality
        for different databases/interfaces. Current version has a focus
        on raw sql queries and stored procedures.

        :param config: configuration dictionary for the database
        :type config: dict
        :param name: dictionary key for the db config being looked up
        :type name: str
        """
        try:
            config = config if config else app.config['db']
        except KeyError:
            raise KeyError(error_messages['missing_db_config'])

        # Set the default class variables for name and type if not set in case
        # the class has those variables cleared or instantiated through handler
        
        if not self.default_db_name:
            set_defaults(config)
        
        name = name if name else self.default_db_name
        
        # Assigns the selected config to class instance
        self.config = config['name']
        
        # Pyndantic model validation to ensure config is correct
        validate_db_config(self.config)

        # Optional, Register methods to the class. We only process instance
        # methods for the init. Class instance methods should be handled
        # seperately from the class instance.
        self.register_instance_methods(methods)
        
        # Sets the rest of the parameters, returns None if not present.
        self._driver = config.get('driver')
        self._dialect = config.get('dialect')
        self._args = config.get('args')
        self._interface = config.get('interface')
        self._path = config.get('path')
        self._params = config.get('params')
        self._uri = config.get('uri')
        
        
        # Output controls
        self._auto_sort = config.get('auto_sort', self._auto_sort)
        self._auto_page = config.get('auto_page', self._auto_page)
        
    def __getattr__(self, name: str):
        try:
            return self.config[name]
        except KeyError:
            raise AttributeError(f"{self.__class__.__name__} object has no attribute '{name}'")  

    def __repr__(self):
        return f"{self.__class__.__name__}({self.config})"

            
    def register_instance_methods(self, methods: Dict[str, Callable]):
        "Registers instance methods"
        methods = self.allowed_methods(methods)
        
        for name, method in methods.items():
            setattr(self, name, method)
    
    @classmethod      
    def register_class_methods(self, methods: Dict[str, Callable]):
        "Registers class methods"
        methods = self.allowed_methods(methods)
        
        for name, method in methods.items():
            setattr(self, name, method) 
            
    @property
    def dialect(self):
        return self._dialect if self._dialect else 'sqlalchemy'
    
    def allowed_methods(self, methods):
        "Add-in logic if you want to only allow specific methods to be registered"
        
        return methods
        
    # @abstractmethod
    # def close(self):
    #     "Closes the database connection. Must be implemented by subclasses."
        
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
        
        dialect = self.dialect

        if dialect == 'sqlite':
            raise ValueError("SQLite does not support stored procedures.")
            
        # SQL templates based on dialect 
        sql_templates = {
            'postgresql': f"CALL {procedure_name}({', '.join([f':{key}' for key in params]) if params else ''})",
            'mysql': f"CALL {procedure_name}({', '.join([f':{key}' for key in params]) if params else ''})",
            'mssql': f"EXEC {procedure_name} {', '.join([f'@{key}=:{key}' for key in params]) if params else ''}",
            'oracle': f"BEGIN {procedure_name}({', '.join([f':{key}' for key in params]) if params else ''}); END;",
        }
        
        procedure_sql = sql_templates[dialect]
        
        # Ensure dialect is supported
        if dialect not in sql_templates:
            raise ValueError("Unsupported database dialect")
        
        # Select the appropriate version of procedure based on dialect
        sp_text = text(procedure_sql)
        
        # Execute the stored procedure
        self.execute(sp_text, params)

    
    def _execute(
        self, 
        sql: str, 
        params:  Union[Dict[str, Any], Tuple[str], None] = None
    ) -> Union[List[Dict[str, Any], Dict[str]]]:
        """Execute SQL queries and returns the results.

        Args:
            sql (str): The SQL query or SP to be executed.
            params (Union[Dict[str, Any], Tuple[str], None]): The parameters to 
                send with the sql query for a formatted string or a stored 
                procedure. Defaults to None. 
            single_row_as_dict (bool, optional): _description_. Defaults to True.

        Returns:
            Union[List[Dict[str, Any], Dict[str]]]: Will return a list of
                dictionaries if there is more than 1 row. Can return either
                a dictionary or list depending on the single_row_as_dict argument.
        """
        try:
            if not self.conn:
                self.conn = self.connect()
            
            # Add paging logic
            query = self._page(sql)
            
            result = self._error_handler(self.execute(query, params))
            
            self.commit()
            
        except Exception as e:
            # More modifications to be made for error handling
            #self.logger.error
            self.rollback()
            
            print(f"An error occurred while executing the query. Details: {e}")
            raise
        # Any post query processing
        return self.process_result(result)
    

    def close(self):
        "Closes the database connection/session"
        return self.conn.close() if self.conn else None
    
    def rollback(self):
        return self.conn.rollback()
    
    def commit(self):
        return self.conn.commit()
    
    def process_result(cls, data: dict) -> dict:
        """
        Subclass override for processing query results. e.g. set default
        behavior for returning a single row as list or dict
        """
        return data
    
    def _page(self, sql):
        return self._sort(sql) if is_stored_procedure(sql) else sql
    
    def _sort(self, sql):
        return sql
        
    def _error_handler(self, data: dict) -> dict:
        "Function to handle errors from execution"
        return data
    
    @classmethod
    def set_auto_page_size(self, page_size):
        self.auto_page_size = page_size
        
    @abstractmethod
    def connect(self) -> Any:
        """Connects to the database. Must be implemented by subclasses."""
    
    @abstractmethod
    def execute(
        self, sql: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Abstract method to be implemented by subclasses to execute the query and fetch results."""
        
    def _read_sql_file(self):
        "For processing .sql files"
        
        
        # To be built
        

class SqAlchemyConn(BaseDB):
    "A default class for database connections using SqlAlchemy"
    def __init__(self, config=None, name=None):
        """
        Creates a database connection from a database configuration

        :param config: database configuration
        :type config: dict
        :param name: key name for the db being called. defaults to None
        :type name: str, optional
        """
        
        try:
            # Read and set connection parameters
            super().__init__(config, name=name)

            
        except KeyError as e:
            raise("Please check your configuration file for a valid database entry.")
    
    @property
    def engine(self):
        "Create a SQLAlchemy engine"
        
        engine_options = self.config.options if self.config.options else {}
        
        return create_engine(self.uri,  **engine_options)
    
    @property
    def uri_base_string():
        return "{driver}://{username}:{password}@{host}:{port}/{database}"
    
    @property
    def uri(self):
        "Returns the connection string for create_engine"
        if self._uri:
            return self._uri
        return self.uri_base_string.format(**self._conn)
    
    def execute(self, sql: str, params: Dict = None) -> Dict | List: 
        
        result = self.conn.execute(text(sql), params)
        
        rows = result.fetchall()
        
        column_names = result.keys()
        
        return result
    
    def connect(self):
        "Opens the connection as a session"
        if self.conn is not None:
            return self.conn
        try:
            self.conn = scoped_session(sessionmaker(bind=self.engine))
            return self.conn
        except OperationalError as e:
            #self.logger.error
            self.conn = None
        except SQLAlchemyError as e:
            print(f"SQLAlchemy error: An error occurred. Details: {e}")
            self.conn = None
        except Exception as e:
            print(f"Unexpected error: {e}")
            self.conn = None
        finally:
            self.close
    
        


################ DEPRECATED #######################
###################################################
class OdbcConn(BaseDB):
    def __init__(self, config=None, name=None):
        """ DEPRECATED
        Creates a basic database connection from a database configuration 
        for ODBC connections. Supports DSN connections. 

        :param config: database configuration
        :type config: dict
        :param name: key name for the db being called. defaults to None
        :type name: str,
        """
        # Set using the db config assigned to app if none is provided.
        
        try:
            # Read and set connection parameters
            super().__init__(config, name=name)
                    
        except KeyError as e:
            raise(error_messages['key_error'])
    
        self._raw = True
        
    def connect(self):
    # Connects to the DB via odbc using the connection string.
    # We return the object and assign itself the connection to provide
    # 2 methods to access the connection
        self.conn = pyodbc.connect(self._uri)
        return self.conn

    @property
    def uri(self):
        
        # Return the connection string if already present
        if self._uri:
            return self._uri
            
        driver = self.params.get('driver')
        connection_str = [f"DRIVER={{{driver}}}"]
        
        try:
            for key, value in self.params.items():
                if isinstance(value, bool):
                    value = 'yes' if value else 'no'
                    connection_str.append(f"{key.upper()}={value}")
        except KeyError as e:
            raise(error_messages['key_error'])
        
        return connection_str
    
    def _execute(self, sql: str, params: Dict = None) -> Dict | List: 
        
        cursor = self.conn.cursor()
        cursor.execute(sql, params or {})
        
        # Fetch all rows
        rows = cursor.fetchall()

        # Get column names
        columns = [column[0] for column in cursor.description]

        # Convert rows into a list of dictionaries
        results = [dict(zip(columns, row)) for row in rows]

        return results
    
dbclass = {
    "odbc": OdbcConn,
    "sqlalchemy": SqAlchemyConn
}



def get_db(config: dict, name: str=None):
    # Check if the configuration has multiple entries and set the db
    if len(config) > 1:
        # Set the default db name for the class if one is desginated.

        if name:
            try:
                config = config[name]
            except ValueError:
                raise ValueError(error_messages['named_db_not_found'])
        else:
            config = next(iter(config.items()))   

def get_type(name: str=None):
    
    if name:
        return app.config['db'][name].get('type')
    
    name = BaseDB.default_db_name
    
    return app.config['db'][name].get('type')


@app.before_request
def _get_session():
    return get_session()

def get_session(name=None):
    if 'db' not in g:
        db_type = get_type(name)
        db_class = dbclass.get(db_type)
        if db_class:
            g.db = db_class(config=app.config['db'], name=name)
            g.db.connect()  # Ensure the connection is established
        else:
            raise ValueError(error_messages['invalid_connector'])
    return g.db

@app.teardown_appcontext
def teardown_db(exception=None):
    # Removes the db connection and closes it when the request is complete
    db = g.pop('db', None)
    if db is not None:
        db.conn.close()
    
def set_flask_db(config: dict):
    "Utilize if you want to warn if multiple database entries are found" \
        "otherwise last entry"
    with app.app_context():
        if not app.config['database']:
            app.config['db'] = config
        else:
            warnings.warn('There are duplicate entries for database. '
                        'Please check your config files.')
        