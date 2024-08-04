import os
import warnings
from flask import current_app as app
from pyaml_env import parse_config


# Retrieve all the valid keys for flask.

_directory = r"\config"

def parse_yaml_files():
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
                
                # Loads the app config first.
                if 'app' in config:
                    app.config.update(config['app'])
                elif filepath == 'app.yaml' or filepath == 'app.yml':
                    app.config.update(config)
                
                # Add the db config to the app.config
                if filepath == 'db.yaml' or filepath == 'db.yml':
                    set_db(config)
                elif 'db' in config:
                    set_db(config.pop('db'))
                
            except Exception as e: 
                print(f"Error parsing {filepath}: {e}")

        elif os.path.isdir(filepath):
            parse_yaml_files(filepath)  # Recursive call


def get_config():
    """Will check if a user has established as config directory before parsing the config files 
    in the default directory
    """
    if 'config_dir' in app.config:
        parse_config(app.config['config_dir'])
    else:
        parse_config()

def set_db(config: dict):
    "Utilize if you want to warn if multiple database entries are found" \
        "otherwise last entry"
    with app.app_context():
        if not app.config['database']:
            app.config['db'] = config
        else:
            warnings.warn('There are duplicate entries for database. '
                        'Please check your config files.')
        