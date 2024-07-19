# -------------------------------------------
# Main libraries
# -------------------------------------------
from flask_restx import Api
from flask import Blueprint, make_response
import logging
# -------------------------------------------
# Importing local libraries
# -------------------------------------------
from core import _logging
from core.utils.format_utils import convert_to_json, convert_to_csv
# -------------------------------------------
# Importing Namespaces
# -------------------------------------------
#from myapp.mgrsearch.mgrsearch import api as mgrsearch_ns
#from myapp.equcal.equcal_api import api as equcal_ns
from sample_app.api import api as sample_app


api = Api(blueprint)

api.add_namespace(awdCreateObjects_ns, path='/awd')
api.add_namespace(enroute_ns, path='/enroute')
#api.add_namespace(equcal_ns, path='/equcal')
api.add_namespace(mgrsearch_ns, path='/mgrsearch')


logger = logging.getLogger(__name__)

@api.errorhandler
def server_error(err):
    """
    Handle error during request.
    """
    logger.exception('An error occurred during a request.')
    logger.exception(err)
    return """
    An internal error occurred: <pre>{}</pre>
    See logs for full stacktrace.
    """.format(err), 500

@api.representation('text/csv')
def csv_mediatype_representation(data, code, headers):
    """
    Assume the data is already marshaled to CSV and just write it
    to the response
    """
    return raw_response(data, code, headers)


@api.representation('application/json')
def json_mediatype_representation(data, code, headers):
    """
    Assume the data is already marshaled to JSON and just write it
    to the response. If it isn't a string, then try to convert it
    to JSON.
    """
    if not isinstance(data, str):
        data = convert_to_json(data)

    return raw_response(data, code, headers)


def raw_response(data, code, headers):
    """
    Assume the data is already marshaled and just writes it to the response.
    """
    if not isinstance(data, str):
        print('Expected string data, but received:')
        print(data)
        data = '"Error: Malformed response"'

    resp = make_response(data, code)
    resp.headers.extend(headers)

    return resp