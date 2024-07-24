import os
import warnings
from flask import current_app as app
from pyaml_env import parse_config


# Retrieve all the valid keys for flask.
valid_flask_keys = set(app.config.keys())

_directory = "\config"

def parse_yaml_files(directory=None):
    """Recursively parses YAML files for flask app configurations to support
    separate configs for different purposes. eg db_config.yaml, app_config.yaml
    

    :param directory: config main directory. Defaults to ROOT/config
    :type directory: str, os.PathLike[str] 
    :param validate_flask_keys: _description_
    :type validate_flask_keys: _type_
    """ 
    
    directory = directory if directory else _directory
    
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)

        if os.path.isfile(filepath) and filename.endswith(('.yaml', '.yml')):
            try:
                config = parse_config(filepath)
                
                if 'db' in config:
                    if 'db' not in app.config:
                        app.config['db'] = config['db']
                    else:
                        warnings.warn("Multiple DB entries found. Ensure your configurations are set properly.")
                        app.config['db'].update(config['db'])
                else:
                    
                        
            except Exception as e: 
                print(f"Error parsing {filepath}: {e}")

        elif os.path.isdir(filepath):
            parse_yaml_files(filepath)  # Recursive call


@app.before_first_request
def _get_config():
    """Will check if a user has established as config directory before parsing the config files 
    in the default directory
    """
    if 'config_dir' in app.config:
        parse_config(app.config['config_dir'])
    else:
        parse_config()

def set_db(db_config: dict):
    "Utilize if you want to warn if multiple database entries are found" \
        "otherwise last entry"
    if not app.config['database']:
        app.config['database'] = db_config
    else:
        warnings.warn('There are duplicate entries for database. '
                      'Please check your config files.')
