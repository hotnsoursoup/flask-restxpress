from flask import Flask
from components.parsers.config_parser import parse_config

def createApp():

    app = Flask(__name__)
    
    with app.app_context():
        
        
        ""

    return app
