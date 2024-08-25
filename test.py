from pyaml_env import parse_config
from pprint import pprint
from typing import List, Dict, Union
import os
#import fastapi
from utils.parsers.config_parser import parse_yaml_files
from models.db_model import validate_db_model,  DatabaseModel, MultiDatabaseModel
from pydantic import RootModel, ValidationError

dir = os.path.dirname(os.path.realpath(__file__))

filepath = os.path.join(dir, "config\\db.yaml")

con = parse_config(filepath)
Pets = RootModel[List[str]]
PetsByName = RootModel[Union[DatabaseModel, Dict[str, DatabaseModel]]]


x = validate_db_model(con)

print(x)
