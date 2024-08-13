from pydantic import BaseModel, field_validator, Field, model_validator, ValidationError
from typing import List, Optional, Dict, Any
import warnings


_warnings = {
    'multi_config_db_files': """Multiple configuration files found with db entries. 
        This may lead to a wrong db being called.""",

    'multiple_default_dbs': "Only one default database is allowed.",
    
    'missing_default_db': """Multiple databases configurations found without 
        a default set. The first entry will be used as the default."""
}

descriptions = {
    "title": "The title of the application.",
    "description": "A description of the application.",
    "enable_logging": "Enable logging for the application.",
    "version": "The version of the application.",
    "host": "The host and port to run the application on.",
    "paths_dir": """The directory containing the paths to load. Defaults
        to \openapi\paths""",
    "db_dir": """The directory containing the database configurations. 
        Defaults to \config""",
    "handlers_dir": """The directory containing the request handlers. There
        are multiple behaviors that can be set. Please refer to the readme.
        Default location is \handlers.""",
    "flask_config": "Configuration settings for the Flask application."
    
}

class AppConfig(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    enable_logging: Optional[bool] = False
    version: Optional[str] = None
    host: Optional[str] = Field(None, description=descriptions['host'])
    paths_dir: Optional[str] = Field(None, description=descriptions['paths_dir'])
    db_dir: Optional[str] = Field("\config", description=descriptions['paths_dir'])
    handlers_dir: Optional[str] = Field("\handlers", description=descriptions['handlers_dir'])
    flask_config: Optional[Dict[str, Any]] = None
    


def validate_app_config(config_data: dict) -> AppConfig:
    try:
        return AppConfig(**config_data)
    except ValidationError as e:
        msg = f"Config validation error: {e}"
        # Logger
        raise
    
    
