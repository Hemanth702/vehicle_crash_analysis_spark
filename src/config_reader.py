import yaml

def read_config(file_path):
    """Read the YAML configuration file."""
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config