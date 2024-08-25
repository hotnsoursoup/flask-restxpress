from typing import Dict
from typing import Optional
from typing import Literal
from typing import Any
from typing import Union
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
from pydantic import ValidationError
from pydantic import RootModel

from utils.utils import warn



# Error and warning messages
messages = {
    'missing_connection': 'Connection information is missing.',
    'invalid_connector': 'Invalid connector specified.',
    'missing_connector_with_connection_string': 'Connector must be specified with a connection string.',
    'missing_dialect': 'Dialect is missing and no connection string provided.',
    'uri_or_params': 'Either `uri` or `params` must be provided for non-sqlite dialects.',
    'uri_and_params': 'Both `uri` and `params` are provided. Uri will be used',
    'sqlite_path': "`path` is required when `dialect` is `sqlite`.",
    'missing_driver': 'Driver is required when using ODBC connections.',
    'invalid_model': "Invalid model. Choices are `single`, `multi`, and `all`."
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

class ConnectionParams(BaseModel):
    driver: Optional[str] = Field(description=descriptions['database_driver'])
    host: Optional[str] 
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    options: Optional[Dict[str, str]] = Field(description=descriptions['options'])
class DatabaseModel(BaseModel):
    """Pydantic model to validate the database configuration has the 
    necessary information to connect to a database.
    
    """
    
    description: Optional[str] = Field(None, description="A description of the database connection.")
    default: Optional[bool] = Field(False, description=descriptions['database_default'])
    dialect: Literal['mysql', 'mssql', 'postgresql', 'oracle', 'sqlite']
    interface: Optional[Literal['sqlalchemy', 'odbc']] = Field('sqlalchemy', description=descriptions['database_type'])
    driver: Optional[str] = Field(None, description=descriptions['database_driver'])
    uri: Optional[str] = Field(None, description=descriptions['database_uri'])
    connection_params: Optional[ConnectionParams] = Field(None, description=descriptions['database_params'])
    auto_commit: Optional[bool] = Field(False, description="Automatically commit transactions.")
    path: Optional[str] = Field(None, description=descriptions['sqlite_path'])
    output: Optional[str] = Field(None, description="The output format for the database connection.")

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
                warn(messages['uri_and_params'])
        if interface == 'odbc' and not driver:
            raise ValueError(messages['missing_driver'])
        return values

class MultiDatabaseModel(RootModel[Dict[str, DatabaseModel]]):
    pass


def validate_db_model(
    db_config: dict[str, Any], 
    model: Literal["single", "multi", "all"] = "single",
    return_model: bool = False
) -> Union[bool, Union[DatabaseModel, MultiDatabaseModel]]:
    """
    Validate the database configuration dictionary with the given 
    database model. 
    
    :param dict db_config: The database configuration dictionary
    :param str model: 
    :param bool return_model: If True, the matching model will be returned.
        The default behavior will be a boolean to see if the model matches
        either model or checks against both models with "all" on "model" param.
    :return bool: True if config matches model
    """
    models = {
        "single": DatabaseModel,
        "multi": MultiDatabaseModel
    }

    def validate_and_return(model_class):
        try:
            model_class.model_validate(db_config)
            return model_class if return_model else True
        except ValidationError:
            return False
    
    # In case a user is importing the database config from within the app
    # config directly. We need to extract just the database configuration.
    if len(db_config) == 1 and 'db' in db_config.keys():
        db_config = db_config['db']
    
    # Checks against both models.
    if model == "all":
        for model_class in models.values():
            if validate_and_return(model_class):
                return validate_and_return(model_class)
        return False
    # Checks against a single database
    else:

        try:
            return validate_and_return(models[model])
        except ValueError:
            raise(messages['invalid_model'])