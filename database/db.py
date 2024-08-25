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



def get_default_db_config(self, config: dict) -> Union[bool, str]:
    """
    Returns the default database configuration from the 
    configuration file. We rely on the config model validation
    to ensure there is only one default db if there is one provided.
    If there is none defined, we choose the first one.
    
    :param config: configuration dictionary for the database
    :type config: dict
    """
    
    if validate_db_config(config):
        return config
    elif validate_db_config(config.values):
        return config.values()
    else:
        for db_name, config in config.items():
            if config[db_name].get('default') == True:
                return config
            
    # Otherwise just grab the first one.
    return next(iter(config.items()))

class BaseDatabaseModel(ABC):
    """
    Default database configuration used when one is not defined. This
    is more for use when you have multiple database configurations.
    """
    
    default_config = None
    
    """
    ORM support is not currently available, but is in consideration for
    future releases
    """
    _orm = False
    
    "Automatic paging of results across all subclasses"
    _auto_page = False
    
    def __init__(self, 
        config: dict=None, 
        name: str=None,
        methods: Dict[str, Callable] = None
        ):
        """      
        The BaseDatabaseConfig class serves as an abstract base class that 
        provides a standardized configuration framework for connecting to 
        various database systems. 
        
        Key Features:
        
        SqlAlchemy engine via config.
            The configuration will generate the uri, add engine options,
            and create/manage the connection.
            
        Better raw sql support.
            Raw sql is not the recommended implementation, nor is stored
            procedure, but there may be use cases. This class allows for
            sql query and parameter construction, sorting, pagination, 
            error handling, and more. 
            
            Subclass and use execute. Within execute, call self._execute
            
            class MyDbClass(BaseDatabaseModel):
            
            def connect(self):
                
                my_connection = #Your connection method here
                
                self.conn = my_connection

                return self.conn
                
            def execute(self, sql, params):
                
                #do things
                
                self._execute(sql, params)
                

        Stored procedure execution by name.
            The stored procedure logic will generate a procedure based 
            on dialect of the database. Note, you may have to prefix 
            your stored procedure with your database (e.g. dbo.my_procedure)
        
            MySubClass.execute_stored_procedure(name, params)
        
        sort: 
            Sort logic can be applied by subclass and defining sort
            
        pagination: 
            Will page results based on page size. You can set automatic
            paging for all routes through the configuration including
            the default behavior. Paging for separate routes can be
            configured within the route configuration.
            
            For global sort settings in config, see example below.
            0 or no setting defined will mean its disabled. Route
            definitions will override these. See documentation for more
            details.

            This will paginate for every 20 results if the total results
            exceed 30.
            
            settings:
                paging:
                    auto_page_size: 20
                    min_page_size: 30
            ------------------------------------------------------------
            
            


        :param dict config: The database configuration
        :param str name: The name (key) of the db to be used
        :param methods Dict[str, Callable] methods: A dictionary of methods
            you want to register to the instance class. Object class
            registrations have to be called explicitly. 
            --> objclass.register_class_methods(methods)
            
        """
        # Legacy implementation, will delete.
        # try:
        #     config = config if config else app.config['db']
        # except KeyError:
        #     raise KeyError(error_messages['missing_db_config'])

        # # Set the default class variables for name and type if not set in case
        # # the class has those variables cleared or instantiated through handler
        
        # if not self.default_db_name:
        #     set_defaults(config)
        
        # name = name if name else self.default_db_name
        
        # # Assigns the selected config to class instance
        # self.config = config['name']
        
        # Pyndantic model validation to ensure config is correct
        validate_db_config(config)

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
        self.settings = config.get('settings')
        
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

    def set_default_config(self, config, name):
                
        if config is None and name is not None:
            config = app.config['db']
        elif self.default_config:
            config = self.default_config
        
    
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
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Execute SQL queries and returns the results.

        Args:
            sql (str): The SQL query or SP to be executed.
            params (Union[Dict[str, Any], Tuple[str], None]): The parameters to 
                send with the sql query for a formatted string or a stored 
                procedure. Defaults to None. 
        Returns:
            Union[List[Dict[str, Any]], Dict[str, Any]]: Will return a list of
                dictionaries if there is more than 1 row. If there is only
                1 row, the result can be returned as a dictionary or a list
                depending on the one_row_output setting in the config.
                
                settings:
                  one_row_output: dict
        """
        try:
            if not self.conn:
                self.conn = self.connect()
            
            # Add paging logic
            query = self._page(sql)
            
            result = self.execute(query, params)
            
            self.commit()
        
        except Exception as e:
            self.rollback()
            self._error_handler(e)
            
        # Any post query processing
        return self.process_result(result)
    

    def close(self):
        """
        Handles the closing of the connection. Subclass and override 
        if conn.close() is not supported by the connector
        """
        if self.conn:
            self.conn.close()
            self.conn = None

    
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
        return self._sort(sql) if is_stored_procedure(sql) else self.page(sql)
    
    def page(self, sql):
        
        
    def sort(self, sql):
        "Override this if you want to provide sorting logic."
        return sql
        
    def error_handler(self, data: dict) -> dict:
        "Function to handle errors from execution. Implement as necessary"
        return data
    
    @classmethod
    def set_auto_page_size(self, page_size):
        "A class method to set the page size for all class instances"
        self.auto_page_size = page_size
        
    @abstractmethod
    def connect(self) -> Any:
        """Connects to the database. Must be implemented by subclasses."""
    
    def execute(
        self, sql: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        "Should be implemented by the subclass"
        return self.conn.execute(sql, params)
        
    def _read_sql_file(self):
        "For processing .sql files"
        
        
        # To be built
        

class SqAlchemyConnection(BaseDatabaseModel):
    def __init__(self, config=None, name=None):
        """
        Creates a SqlAlchmyDatabase object from a configuration file
        that will provide base functionality to connect to and query
        a database using raw sql queries and stored procedures.
        
        ORM support will be in future releases.
        

        :param _type_ config: _description_, defaults to None
        :param _type_ name: _description_, defaults to None
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
        
        result = self._execute(text(sql), params)
        
        rows = result.fetchall()
        
        column_names = result.keys()
        
        return result
    
    def connect(self):
        """
        Opens the connection as a session. The connection is then
        returned, but also accessible as conn. 
        """
        if self.conn is not None:
            return self.conn
        
        self.conn = scoped_session(sessionmaker(bind=self.engine))
        return self.conn


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


