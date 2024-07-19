from pydantic import BaseModel, field_validator, Field, model_validator, ValidationError
from typing import List, Optional
import warnings


_warnings = {
    'multi_config_db_files': 'Multiple configuration files found with db entries. This may lead to a wrong db being called.',
    'missing_default_db': 'Multiple databases configurations found without a default set. The first entry will be used as the default.'
}

class DatabaseConfig(BaseModel):
    host: Optional[str]
    port: Optional[int]
    user: Optional[str]
    password: Optional[str]
    default: Optional[bool] = False
    args: Optional[list]
    
class AppConfig(BaseModel):
    debug: bool
    databases: List[DatabaseConfig]
    allowed_hosts: List[str]
    admin_email: Optional[str] = None

    @field_validator('databases')
    def check_one_default_database(cls, databases):
        default_databases = [db for db in databases if db.default]
        if len(default_databases) > 1:
            raise ValueError('There must be at most one default database.')
        return databases
    
class MyModel(BaseModel):
    key1: Optional[str] = None
    key2: Optional[str] = None
    key3: Optional[str] = None

    @model_validator(mode='before')
    def check_keys(cls, values):
        key1 = values.get('key1')
        key2 = values.get('key2')
        key3 = values.get('key3')

        if key1:
            # If key1 is present, key2 and key3 are optional
            return values
        else:
            # If key1 is not present, key2 and key3 are required
            if not key2:
                raise ValueError('key2 is required when key1 is not present')
            if not key3:
                raise ValueError('key3 is required when key1 is not present')
        return values


def load_yaml_config(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def validate_config(config_data: dict) -> AppConfig:
    return AppConfig(**config_data)

# Example usage
if __name__ == "__main__":
    yaml_config_path = 'config.yaml'  # Path to your YAML file

    try:
        config_data = load_yaml_config(yaml_config_path)
        app_config = validate_config(config_data)
        print("Configuration is valid:")
        print(app_config)
        default_db = app_config.get_default_database()
        print("Default database is:")
        print(default_db)
    except ValidationError as e:
        print("Configuration is invalid:")
        print(e.json())
    except Exception as e:
        print("An error occurred:")
        print(e)
