import importlib
import sys
from pathlib import Path
from typing import Callable
from typing import Optional

from pydantic import BaseModel
from pydantic import ValidationError
from pydantic import model_validator


class EndpointConfig(BaseModel):
    name: str
    url: str
    method: str
    headers: Optional[dict] = None
    params: Optional[dict] = None
    function_name: str  # Name of the function as a string
    module_path: Optional[str] = 'functions'  # Path to the module as a string
    function: Optional[Callable] = None  # The function itself

    @model_validator(mode='before')
    def load_function(cls, values):
        function_name = values.get('function_name')
        module_path = values.get('module_path')
        
        if not function_name:
            raise ValueError("Function name must be provided")

        # Dynamically load the handler function from the specified module
        try:
            # If module_path is a file path, convert it to a module name
            if Path(module_path).is_file():
                sys.path.insert(0, str(Path(module_path).parent))
                module_name = Path(module_path).stem
            else:
                module_name = module_path
            
            module = importlib.import_module(module_name)
            func = getattr(module, function_name)
            if not callable(func):
                raise ValueError(f"{function_name} is not a callable function")
            values['function'] = func
            return values
        except (AttributeError, ModuleNotFoundError) as e:
            raise ValueError(f"Error loading function '{function_name}' from '{module_path}': {e}")
