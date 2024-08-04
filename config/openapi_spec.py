import yaml
from openapi_schema_validator import OAS30Validator
from jsonschema import ValidationError

def load_yaml(file_path):
    with open(file_path, 'r') as file:
        return yaml.safe_load(file)

def validate_openapi_spec(openapi_spec):
    try:
        validator = OAS30Validator(openapi_spec)
        validator.validate(openapi_spec)
        print("OpenAPI specification is valid.")
    except ValidationError as e:
        print("OpenAPI specification is invalid:", e)

if __name__ == "__main__":
    spec_path = './openapi.yaml'  # Replace with your OpenAPI spec path
    openapi_spec = load_yaml(spec_path)
    validate_openapi_spec(openapi_spec)

