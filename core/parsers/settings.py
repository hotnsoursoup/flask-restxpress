import os
import glob
import ast

def find_settings_variables(directory):
    settings_files = glob.glob(os.path.join(directory, '**', 'settings.py'), recursive=True)
    settings_list = []

    for file_path in settings_files:
        with open(file_path, 'r') as file:
            try:
                file_content = file.read()
                parsed_content = ast.parse(file_content, filename=file_path)
                for node in ast.walk(parsed_content):
                    if isinstance(node, ast.Assign):
                        for target in node.targets:
                            if isinstance(target, ast.Name) and target.id.endswith('_settings'):
                                value = ast.literal_eval(node.value)
                                settings_list.append(value)
            except (SyntaxError, ValueError) as e:
                print(f"Error parsing {file_path}: {e}")

    return settings_list

# Example usage
directory = 'path/to/your/folder'
settings_variables = find_settings_variables(directory)
print(settings_variables)
