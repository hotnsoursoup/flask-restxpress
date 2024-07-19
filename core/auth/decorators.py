from functools import wraps

from flask import request, abort

from core.db import get_db
from core.utils.format_utils import format_marshaled_data


def get_apiauth_object_by_key(key):
    """
    A simplied method for verifying if an api_key provided by the 
    request exist in a security table. Definitely not production quality
    """
    sql = "SELECT user from `ds_events`.rest_api_users where private = ?"
    db = get_db()
    # We can check the result or just reutrn to the conn and check if
    # there is just one row (meaning theres a result)
    with db.conn.cursor() as cur:
        cur.execute(sql, key)
        try:
            cur.fetchone()[0]
            return True
        except: 
            return False

def authorized(key):
    """
    Query the datastorage for an API key.
    @param ip: ip address
    @return: apiauth sqlachemy object.
    """
    if key is None:
      return False
    api_key = get_apiauth_object_by_key(key)
    if api_key:
      return True
    return False

def require_app_key(f):
    """
    @param f: flask function
    @return: decorator, return the wrapped function or abort json object.
    """
    @wraps(f)
    def decorated(*args, **kwargs):
        if authorized(request.headers.get('api_key')):
            return f(*args, **kwargs)
        else:
            abort(401)
    return decorated
