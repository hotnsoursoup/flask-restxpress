import yaml

def yaml_to_markdown(yaml_path: str, markdown_path: str) -> None:
    with open(yaml_path, 'r') as yaml_file:
        data = yaml.safe_load(yaml_file)

    markdown_lines = []
    markdown_lines.append("# Configuration Schema for MyLibrary\n")

    for key, value in data.items():
        if isinstance(value, dict):
            markdown_lines.append(f"## {key.capitalize()}\n")
            for sub_key, sub_value in value.items():
                markdown_lines.append(f"- **{sub_key}**: {sub_value}")
        elif isinstance(value, list):
            markdown_lines.append(f"## {key.capitalize()}\n")
            for item in value:
                markdown_lines.append(f"- {item}")
        else:
            markdown_lines.append(f"- **{key}**: {value}")

    with open(markdown_path, 'w') as markdown_file:
        markdown_file.write('\n'.join(markdown_lines))

# Convert YAML to Markdown
yaml_to_markdown('mylibrary/config_schema.yaml', 'docs/configuration.md')


def append_to_readme(readme_path: str, schema_path: str) -> None:
    with open(readme_path, 'a') as readme_file, open(schema_path, 'r') as schema_file:
        readme_file.write("\n## Configuration Schema\n")
        readme_file.write(schema_file.read())

# Append schema to README
append_to_readme('README.md', 'docs/configuration.md')