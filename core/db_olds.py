__docformat__ = "restructuredtext"

import pyodbc
import re
import logging 
import pandas as pd

from werkzeug.datastructures import ImmutableDict as iDict
from flask import g, current_app as app
from sqlalchemy import create_engine

from core.format_utils import lowercase_all
from definitions import DB_FIELDS, DB_CONN_SQLA


logger = logging.getLogger('error')

write_cmd = ('UPDATE', 'INSERT', 'DELETE')



def format_query(query, format_args=None):
    # Formats the query string with given arguments
    
    # Lowercase ensures consistency when formatting the args
    query = query.lower()
    
    if format_args == None:
        return query
    elif isinstance(format_args, list):
        return query.format(*format_args)
    elif isinstance(format_args, (dict, iDict)):
        return query.format(**format_args)        
    else:
        return query.format(format_args)


def build_mysql_procedure(procedure, args):
    "MySql Stored procedure configuration."
    if isinstance(args, str):
        return f"CALL {procedure} ('{args}')"
    elif len(args) == 1 and isinstance(args, list):
        return f"CALL {procedure} ('{args[0]}')"
    elif isinstance(args, dict):
        values = list(args.values())
    else:
        values = args
        
    # Create formatting string for args    
    arg_values = f"""('{"', '".join(map(str, values))}')"""
    
    # Fixes for None values and empty strings for stored procedures that
    # have optional values
    arg_values = arg_values.replace("'None'", 'NULL').replace("''''", "''")

    # Return formatted query string
    return f"CALL {procedure}{arg_values}"

def is_write_cmd(query):
    # Checks if the query is an insert, update, or delete.
    # Some select statements may be built using CTE(common table expressions)
    # so they may not always start with select
    
    return True if query.strip().upper().startswith(write_cmd) else False


def trim_string(string, trim_carriage=True):
    # Trims query string carriage returns for better logging in 
    # case you have it imported using a formatter. You can also add 
    # back in carriage returns for keywords only as well.
    
    if trim_carriage:
        string = ' '.join(string.split())
    else:
        string = re.sub(' +', ' ', string)
    return string


def log(query):
    logger.debug(f'{trim_string(string=query)}')


db_type = app['config']['database']['db_type']

    
class DbConnect:
    # Database connection object
    
    def __init__(self):
        # Allowed connection types: odbc, dsn, sqlalchemy
        self.conn_method = app['config']['database']['conn_type']
        
        # Supported db_types: mssql, mysql
        self.db_type = app['config']['database']['db_type']
        
    @property
    def basestring(self):
        return "Driver=%s;Server=%s;Port=%s;UID=%s;PWD=%s;auth_plugin='mysql_native_password'"
    
    #@property
    #def engine(self):
    #     return create_engine("mysql://%s:%s@%s:%s/".format(DB_CONN_SQLA))
    
    @property 
    def dbstring(self):
        # Build a connection string from the config yaml file
        return (self.basestring % (DB_FIELDS))
    
    def connect(self):
        # Connects using the DB via PYODBC using the connection string
        # We return the object and assign itself the connection to provide
        # 2 methods to access the connection
        self.conn = pyodbc.connect(self.dbstring)
        return self.conn

    def execute(self, query=None, args=None, procedure=None):
        # Used for delete, update, insert, etc.
        try:
            with self.conn as conn:
                
                if procedure:
                    # Get the procedure sql string
                    query = build_procedure_sql_args(procedure, args)
                else:
                    # Format the query string
                    query = format_query(query, args)
            
                if not is_write_cmd(query) and g.request.method == 'GET':
                    # Results are returned as a list of dictionaries
                    df = pd.read_sql(query, conn)
                    if not df.empty:
                        result = df.to_dict(orient='records')

                        result = lowercase_all(result)
                        
                        return result[0] if len(result) == 1 else result
                    
                    return None
                else:
                    with conn.cursor() as cursor:
                        # This will execute the write command, but it is important
                        # that you commit the changes to the database with validation
                        # in the custom function
                        cursor.execute(query)
                    
                    
        except pyodbc.Error as e:
            msg = e.args[1]
            logger.error(msg)
            logger.error(query)

    def commit(self):
        self.conn.commit()


def get_db():
    # Grabs the db object within the application context using g
    if 'db' not in g:
        g.db = DbConnect()
        g.db.connect()
    return g.db


@app.teardown_appcontext
def teardown_db(exception=None):
    # Removes the db connection and closes it when the request is complete
    db = g.pop('db', None)
    if db is not None:
        db.conn.close()
  
