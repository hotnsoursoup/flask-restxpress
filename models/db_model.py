from typing import Dict, Optional, Union, Literal, Any
from pydantic import BaseModel, Field, field_validator, model_validator, RootModel


import warnings


# Error and warning messages
messages = {
    'missing_connection': 'Connection information is missing.',
    'invalid_connector': 'Invalid connector specified.',
    'missing_connector_with_connection_string': 'Connector must be specified with a connection string.',
    'missing_dialect': 'Dialect is missing and no connection string provided.',
    'uri_or_settings': 'Either `uri` or `settings` must be provided for non-sqlite dialects.',
    'uri_and_settings': 'Both `uri` and `settings` are provided. Uri will be used',
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
        
    "database_settings": """Connection settings for the database. Host
        Username, password, etc are stored here.""",
        
    "sqlite_path": """The path to the SQLite database file.""",
    
    "database_driver": """The driver to use for ODBC connections or 
        driver for sqlalchemy connections.""",
        
    "options": """A dictionary of keyword args to send to create_engine()
        if using sqlalchemy (default), or for odbc connections, a dictionary
        of connection string parameters"""
}

class Settings(BaseModel):
    driver: Optional[str] = Field(description=descriptions['database_driver'])
    host: str
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
    type: Optional[Literal['sqlalchemy', 'odbc']] = Field('sqlalchemy', description=descriptions['database_type'])
    uri: Optional[str] = Field(None, description=descriptions['database_uri'])
    settings: Optional[Settings] = Field(None, description=descriptions['database_settings'])
    path: Optional[str] = Field(None, description=descriptions['sqlite_path'])
    output: Optional[str] = Field(None, description="The output format for the database connection.")

    @model_validator(mode='before')
    def check_dialect_requirements(cls, values):
        dialect = values.get('dialect')
        _type = values.get('type')
        
        if dialect == 'sqlite':
            if not values.get('path'):
                raise ValueError(messages['sqlite_path'])
        else:
            if not (values.get('uri') or values.get('settings')):
                raise ValueError(messages['uri_or_settings'])
            if values.get('uri') and values.get('settings'):
                warnings.warn(messages['uri_and_settings'])
        if _type == 'odbc' and not values.get('driver'):
            raise ValueError(messages['missing_driver'])
        return values


    
def validate_db_config(config_data: dict) -> DatabaseConfig:
    return DatabaseConfig(**config_data)

