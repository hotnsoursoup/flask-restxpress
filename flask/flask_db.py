from flask import current_app as app
from flask import g

from models.db_model import DatabaseModel
from models.db_model import MultiDatabaseModel


def get_flask_db(config: dict=None, name: str=None):
    # Check if the configuration has multiple entries and set the db
    if 
        # Set the default db name for the class if one is desginated.

        if name:
            try:
                config = config[name]
            except ValueError:
                raise ValueError(error_messages['named_db_not_found'])
        else:
            config = next(iter(config.items()))   



@app.before_request
def _get_session():
    return get_session()


def get_session(name=None):
    if 'db' not in g:
        g.db = 
    return g.db

@app.teardown_appcontext
def teardown_db(exception=None):
    # Removes the db connection and closes it when the request is complete
    db = g.pop('db', None)
    if db is not None:
        db.conn.close()
    
def set_flask_db(config: dict):
    "Utilize if you want to warn if multiple database entries are found" \
        "otherwise last entry"
    with app.app_context():
        if not app.config['database']:
            app.config['db'] = config
        else:
            warnings.warn('There are duplicate entries for database. '
                        'Please check your config files.')
