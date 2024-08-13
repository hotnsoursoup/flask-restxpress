import os
import warnings
from pyaml_env import parse_config

# Retrieve all the valid keys for Flask.
_directory = "config"

def parse_yaml_files(app, directory=None):
    """Recursively parses YAML files for flask app configurations to support
    separate configs for different purposes. eg db_config.yaml, app_config.yaml
    
    :param directory: config main directory. Defaults to ROOT/config
    :type directory: str, os.PathLike[str] 
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
                elif filename in ['app.yaml', 'app.yml']:
                    app.config.update(config)
                
                # Add the db config to the app.config
                if filename in ['db.yaml', 'db.yml']:
                    app.config['db'].update(config)
                elif 'db' in config:
                    app.config['db'].update(config['db'])
                
            except Exception as e: 
                print(f"Error parsing {filepath}: {e}")

        elif os.path.isdir(filepath):
            parse_yaml_files(app=app, directory=filepath)  # Recursive call with correct directory

def update_config(app):
    """Will check if a user has established a config directory before parsing the config files 
    in the default directory.
    """
    app.config['db'] = {}
    
    if 'config_dir' in app.config:
        parse_yaml_files(app=app, directory=app.config['config_dir'])
    else:
        parse_yaml_files(app=app)
