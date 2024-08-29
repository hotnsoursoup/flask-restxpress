from utils.parsers.config_parser import parse_yaml_files


def update_config(app):
    """Will check if a user has established a config directory before parsing the config files 
    in the default directory.
    """
    app.config['db'] = {}
    
    if 'config_dir' in app.config:
        parse_yaml_files(app=app, directory=app.config['config_dir'])
    else:
        parse_yaml_files(app=app)
