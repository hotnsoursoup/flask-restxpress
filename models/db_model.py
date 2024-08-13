from typing import Dict, Optional, Literal, Any
from pydantic import BaseModel, Field, model_validator, ValidationError


import warnings


# Error and warning messages
messages = {
    'missing_connection': 'Connection information is missing.',
    'invalid_connector': 'Invalid connector specified.',
    'missing_connector_with_connection_string': 'Connector must be specified with a connection string.',
    'missing_dialect': 'Dialect is missing and no connection string provided.',
    'uri_or_params': 'Either `uri` or `params` must be provided for non-sqlite dialects.',
    'uri_and_params': 'Both `uri` and `params` are provided. Uri will be used',
    'sqlite_path': "`path` is required when `dialect` is `sqlite`.",
    'missing_driver': 'Driver is required when using ODBC connections.'
}


# Field descriptions
descriptions = {
    "database_type": """The type of database connection. Can be ODBC 
        or SQLAlchemy. Defaults to SQLAlchemy.""",
    
    "database_default": """Used when identifying the default database loaded 
        used by all connections without an explicitly defined DB""",
        
    "database_uri": """For SQL Alchemy, this can be the connection string
        or without arguments. For ODBC, this is the DSN name.""",
        
    "database_params": """Connection string params for the database. Host
        Username, password, etc are stored here.""",
        
    "sqlite_path": """The path to the SQLite database file.""",
    
    "database_driver": """The driver to use for ODBC connections or 
        driver for sqlalchemy connections.""",
        
    "options": """A dictionary of keyword args to send to create_engine()
        if using sqlalchemy (default), or for odbc connections, a dictionary
        of connection string parameters"""
}

class DbParams(BaseModel):
    driver: Optional[str] = Field(description=descriptions['database_driver'])
    host: Optional[str] 
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    options: Optional[Dict[str, Any]] = Field(description=descriptions['options'])
    

class DatabaseConfig(BaseModel):
    """Pydantic model to validate the database configuration has the 
    necessary information to connect to a database.
    
    """
    
    description: Optional[str] = Field(None, description="A description of the database connection.")
    default: Optional[bool] = Field(False, description=descriptions['database_default'])
    dialect: Literal['mysql', 'mssql', 'postgresql', 'oracle', 'sqlite']
    interface: Optional[Literal['sqlalchemy', 'odbc']] = Field('sqlalchemy', description=descriptions['database_type'])
    driver: Optional[str] = Field(description=descriptions['database_driver'])
    uri: Optional[str] = Field(None, description=descriptions['database_uri'])
    params: Optional[DbParams] = Field(None, description=descriptions['database_params'])
    auto_commit: Optional[bool] = Field(False, description="Automatically commit transactions.")
    path: Optional[str] = Field(None, description=descriptions['sqlite_path'])
    output: Optional[str] = Field(None, description="The outp   ut format for the database connection.")

    @model_validator(mode='before')
    def check_dialect_requirements(cls, values):
        dialect = values.get('dialect')
        interface = values.get('interface')
        uri = values.get('uri')
        params = values.get('params')
        
        # Check for driver. Driver can be present in 2 locations.
        driver = values.get('driver')
        if not driver:
            driver = params.get('driver') if params else None
        
        # sqlite requires a path
        if dialect == 'sqlite':
            if not values.get('path'):
                raise ValueError(messages['sqlite_path'])
        else:
            if not (uri or params):
                raise ValueError(messages['uri_or_params'])
            if uri and params:
                warnings.warn(messages['uri_and_params'])
        if interface == 'odbc' and not driver:
            raise ValueError(messages['missing_driver'])
        return values
    
def validate_db_config(config_data: dict) -> DatabaseConfig:
    try:
        return DatabaseConfig(**config_data)
    except ValidationError as e:
        msg = f"Config validation error: {e}"
        # Logger
        raise
    