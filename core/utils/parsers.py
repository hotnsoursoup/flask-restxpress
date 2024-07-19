from flask_restx import reqparse


def keyParser(args):
    "A parser to parse arguments and validate accepted values"
    
    # This is antiquated, but it works for what we need it to do
    parser = reqparse.RequestParser()
    
    # Loops for managing a list of args or a dictionary of args
    if type(args) == list:
        for arg in args:
            parser.add_argument(**arg)
    elif type(args) == dict:
        parser.add_argument(**args)
    else:
        raise('Keyparser only accepts list or dictionary objects')
    return parser

