from .functions import get_user
from auth import auth_method

settings = {
    'name': 'user',
    'parent_name': 'enroute',
    'auth_method' auth_method,
    'route': '/user',
    'methods': 'GET',
    'custom_functions': {
        'GET': get_user
        }, 
    'args':  [
        {
            'name': 'id',
            'required': True,
            'location': 'args', 
            'type': str
        },
        {
            'name': 'internal_id',
            'required': False,
            'location': 'args', 
            'type': str
        }
    ]
}
