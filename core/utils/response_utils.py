from logging import Logger
import re
from typing import Any, Dict, List, Union

from flask import Response, abort, jsonify, make_response

from core.db import get_db
from core.utils.format_utils import capitalize



def response(status: int, 
             response: Union[str, List[Dict[str, str]], Dict[str, str]], 
             mimetype: str = 'application/json'
             ) -> Response:
    
    # Helps to create a Response object and handle certain types of 
    # conditions that may arise during the response creation process and
    # logs them to the given logger.
    
    if isinstance(response, str):
        "Ensure the response string has a capital letter."
        response = capitalize(response)
    
    if status == 200:
        if isinstance(response, str):
            return Response(response, status=status, mimetype=mimetype)
        else:
            return Response(response, status=status, mimetype=mimetype)
    else:
        if isinstance(response, str):
            return Response(response, status)
        else:
            return Response(500, "Unknown Server Error. Try again later.")


def format_response(data: Union[Any, List[Dict[str, Any]], Dict[str, Any]]) -> Response:
    if isinstance(data, Response):
        return data
    if data and len(data) > 0:
        return make_response(data, 200)
    else:
        return empty_response()


def empty_response():
    return make_response(jsonify({}), 404)


def check_response(result: Any) -> bool:
    return True if isinstance(result, Response) else False


def has_results(query: str, args: Union[List[str], Dict[str, str]]) -> bool:
    """Returns true if the query returns at least one result"""
    db = get_db()
    query = query + ' LIMIT 1'
    result = db.execute(query, args)
    return len(result) >= 1


def in_payload(field, fields):
    return True if field in fields else False


def get_arguments(request) -> Union[List[str], Dict[str, Any]]:
    if request.method == 'GET':
        return request.args
    elif request.method == 'POST':
        return request.json
    elif request.method == 'PUT':
        return request.form
    elif request.method == 'PATCH':
        return request.json
    elif request.method == 'DELETE':
        return request.args


def get_sample_data(data: Union[List[Dict[str, Any]], Dict[str, Any], List[str]]) -> List[Any]:
    if isinstance(data, list):
        if type(data[0]) == dict: 
            return list(data[0].values())[:5]
        return list(data[0])[:5]
    elif isinstance(data, dict):
        return list(data.values())[:5]


def get_argument_names(args: Union[List[Dict[str, Any]], Dict[str, Any]]) -> List[str]:
    if isinstance(args, list):
        arg_names = []
        for arg in args:
            arg_names.append(arg['name'])
        return arg_names
    else:
        return [args['name']]
