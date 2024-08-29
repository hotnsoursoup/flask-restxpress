from typing import Any
from typing import Dict
from typing import Literal
from typing import Optional
from typing import Union

from pydantic import BaseModel
from pydantic import Field
from pydantic import RootModel
from pydantic import ValidationError
from pydantic import model_validator

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
    "database_default": 
        """
        Used when identifying the default database loaded 
        used by all connections without an explicitly defined DB
        """,
        
    "database_uri": 
        """
        For SQL Alchemy, this can be the connection string
        or without arguments. For ODBC, this is the DSN name.
        """,
        
    "database_params": 
        """
        Connection string params such as host, username, password, etc 
        are stored here.for the database. 
        """,
        
    "sqlite_path": "The path to the SQLite database file.",
    
    "database_driver": 
        """
        The driver to use with the connection. Driver definition may 
        vary between dialects and if odbc support is enabled.
        """,
        
    "options": 
        """
        A dictionary of options to send to create_engine(). 
        """,
    
    "use_odbc": "Enable the ODBC support."
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
    use_odbc: Optional[bool] = Field(False, description=descriptions['use_odbc'])
    driver: Optional[str] = Field(None, description=descriptions['database_driver'])
    uri: Optional[str] = Field(None, description=descriptions['database_uri'])
    connection_params: Optional[ConnectionParams] = Field(None, description=descriptions['database_params'])
    auto_commit: Optional[bool] = Field(False, description="Automatically commit transactions.")
    path: Optional[str] = Field(None, description=descriptions['sqlite_path'])
    output: Optional[str] = Field(None, description="The output format for the database connection.")

    
    @model_validator(mode='before')
    def field_validation(cls, values):
        
        if len(values.keys()) == 1:
            values = next(iter(values.values()))

        dialect = values.get('dialect')
        uri = values.get('uri')
        params = values.get('params')
        use_odbc = values.get('use_odbc')
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
        if use_odbc and not driver:
            raise ValueError(messages['missing_driver'])
        return values

class MultiDatabaseModel(RootModel[Dict[str, DatabaseModel]]):
    """
    Model for multidatabase configurations. Conifigurations that
    are defined with a root key (database name) and only have 1 value
    will pass validation.
    
    e.g.
    
    mydatabase1:
      dialect: mysql
      uri: mysql://user:pass
      
    """
    pass


models = {
        "single": DatabaseModel,
        "multi": MultiDatabaseModel
    }


def validate_db_model(
    db_config: dict[str, Any], 
    model: Literal["single", "multi", "any"] = "single",
    return_model: bool = False,
) -> Union[bool, Union[DatabaseModel, MultiDatabaseModel, None]]:
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
    
    
    # In case the database config is nested within the config being passed in.
    if 'db' in db_config:
        db_config = db_config['db']
    
    def validate_and_return(model_class):
        try:
            validated_model = model_class.model_validate(db_config)
            return validated_model if return_model else True
        except ValidationError:
            return False
        
    
    if model in models:
        return validate_and_return(models[model])
    else:
        for model_class in models.values():
            if validate_and_return(model_class):
                return validate_and_return(model_class)
        return False

        
    
def get_nested_config(config: dict) -> dict:
    """
    Retrieves nested database configurations

    :param _type_ config: The root database configuration
    :return dict: 
    """
    
    if 'db' in config.keys():
        config = config['db']
    
    return next(iter(config.items())) if len(config) == 1 else config