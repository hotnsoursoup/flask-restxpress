import os
import warnings
from flask import app
from pyaml_env import parse_config


# Retrieve all the valid keys for flask.
valid_flask_keys = set(app.config.keys())


def parse_yaml_files(directory, validate_flask_keys):
    """Recursively parses YAML files for flask app configurations to support
    separate configs for different purposes. eg db_config.yaml, app_config.yaml
    

    :param directory: config main directory. Defaults to ROOT/config
    :type directory: str, os.PathLike[str] 
    :param validate_flask_keys: _description_
    :type validate_flask_keys: _type_
    """ 
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)

        if os.path.isfile(filepath) and filename.endswith(('.yaml', '.yml')):
            try:
                config = parse_config(filepath)
                
                # Validate assigned flask keys
                if 'flask' in config.keys():
                    for key, value in config['app'].items():
                        if key in valid_flask_keys:
                            app.config[key] = value
                else:
                    # Remove app from the loop to avoid duplication
                    config.pop('app')
                    # Assign the rest to the flask config
                    for key, value in config.items():
                        app.config[key] = value
            
            except Exception as e: 
                print(f"Error parsing {filepath}: {e}")

        elif os.path.isdir(filepath):
            parse_yaml_files(filepath)  # Recursive call
            



def set_db(db_config: dict):
    "Utilize if you want to warn if multiple database entries are found" \
        "otherwise last entry"
    if not app.config['database']:
        app.config['database'] = db_config
    else:
        warnings.warn('There are duplicate entries for database. '
                      'Please check your config files.')
