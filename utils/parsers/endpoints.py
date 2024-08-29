import logging
from typing import Any
from typing import Union

from flask_restx import Namespace

from components.factories.templates import Endpoint


logger = logging.getLogger('endpoint')

def load_endpoint_settings(api: Namespace, 
                           settings: Union[list[dict], dict[str, Any]]
                           ):
    """ Iterate through and load each endpoint settings dictionary"""

    if isinstance(settings, dict):
        # Create the api object for each
        endpoint = Endpoint(api=api, settings=settings)

        # Initiate the object
        endpoint.create_endpoint()
        
    elif isinstance(settings, list):
        for setting in settings:
            #Loop through each and load
            load_endpoint_settings(api=api,settings=setting)
