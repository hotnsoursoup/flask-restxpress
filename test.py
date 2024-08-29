import os
from pprint import pprint
from typing import Dict
from typing import List
from typing import Union

from pyaml_env import parse_config
from pydantic import RootModel
from pydantic import ValidationError

from models.db_model import DatabaseModel
from models.db_model import MultiDatabaseModel
from models.db_model import validate_db_model

#import fastapi
from utils.parsers.config_parser import parse_yaml_files


dir = os.path.dirname(os.path.realpath(__file__))

filepath = os.path.join(dir, "config\\db.yaml")

con = parse_config(filepath)

x = validate_db_model(con, "single")

print(x)
