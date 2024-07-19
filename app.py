from flask import Flask
from definitions import PROD_CONFIG, DEV_CONFIG, TEST_CONFIG, APP_URL_PREFIX


def createApp():

    app = Flask(__name__)
    with app.app_context():
        
        app.config.from_object(PROD_CONFIG)
        from sample_app import blueprint as api
        app.register_blueprint(api, url_prefix=APP_URL_PREFIX)

    return app
