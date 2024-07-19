from pydantic import BaseModel, Field, model_validator, ValidationError
from typing import Optional, Dict, Any, List

# Example allowed connectors
allowed_connectors = ['connector1', 'connector2']  

# Error messages
error_messages = {
    'missing_connection': 'Connection information is missing.',
    'invalid_connector': 'Invalid connector specified.',
    'missing_connector_with_connection_string': 'Connector must be specified with a connection string.',
    'missing_dialect': 'Dialect is missing and no connection string provided.'
}

class DBConfig(BaseModel):
    connection: Dict[str, Any]
    connector: Optional[str] = None
    driver: Optional[str] = None
    connection_string: Optional[str] = None
    dialect: Optional[str] = None
    args: Optional[List[str, Dict]] = None
    params: Optional[List[str, Dict]] = None
    function: Optional[str] = None

    @model_validator(mode='before')
    def check_connection(cls, values):
        connection = values.get('connection')
        if not connection:
            raise ValueError(error_messages['missing_connection'])
        
        connector = connection.get('connector')
        connection_string = connection.get('connection_string')
        dialect = connection.get('dialect')

        if connector and connector not in allowed_connectors:
            raise ValueError(error_messages['invalid_connector'])
        if connection_string and not connector:
            raise ValueError(error_messages['missing_connector_with_connection_string'])
        if not dialect and not connection_string:
            raise ValueError(error_messages['missing_dialect'])

        # Set values from the connection dictionary
        values['connector'] = connection.get('connector')
        values['connection_string'] = connection.get('connection_string')
        values['dialect'] = connection.get('dialect')
        values['driver'] = connection.get('driver')
        values['args'] = connection.get('args')
        values['params'] = connection.get('params')
        values['function'] = connection.get('function')
        
        return values

# Example usage
config_data = {
    'connection': {
        'connector': 'connector1',
        'connection_string': 'some_connection_string',
        'dialect': 'mysql',
        'driver': 'some_driver',
        'params': {'param1': 'value1'},
    'args': {'arg1': 'value1'},
    'function': 'some_function'
    }
}

try:
    db_config = DBConfig(**config_data)
    print("Configuration is valid:")
    print(db_config)
except ValidationError as e:
    print("Configuration is invalid:")
    print(e)
