
from flask_restx import Namespace

from sample_app.store.endpoints import customer_info1_config, orders_config
from sample_app.api_template import Endpoint


store_namespace = Namespace('store', description='My store data')
""" 
Custom function to test the app before querying / configuring DB
"""
def customFunction(db=None, **kwargs):
    x =  {"note": "This is a custom function."}
    for k,v in kwargs.items():
        x[k] = v
    return x

########################################################################
# Endpoints were designed to simplify the deployment. Complex ones can
# still be deployed using the standard method. 
#
# /myapp/store/endpoints --> Settings files
# The settings file is a dictionary object that contains
# - Route
# - Arguments
########################################################################

customer_endpoint = Endpoint(
    api=api, 
    settings=customer_info1_config, 
    custom_function=customFunction
    )         
customer_endpoint.load_endpoint()

order_endpoint = Endpoint(
    api=api, 
    settings=orders_config, 
    custom_function=customFunction
    )                              

order_endpoint.load_endpoint()


""" Example standard implementation

parser = reqparse.RequestParser()
parser.add_argument('api_key', required=True, location='headers')
parser.add_argument('id', required=True, location='args')

@api.route('/customer', methods=['GET'])
class myClass(Resource):

    def get(self, args):
        # do things


"""