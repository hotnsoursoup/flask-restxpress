from flask import Flask

from utils.parsers.config_parser import update_config


def createApp():

    app = Flask(__name__)
    
    with app.app_context():
        
        update_config(app)
        
        print(app.config['db'])
        
        print(app.config)
        
    
    
createApp()
