import yaml
from typing import Dict, Any


def read_config(filepath: str) -> Dict[str, Any]:
    """
    Reads a YAML configuration file and returns its contents as a dictionary.

    This function opens a file from the given filepath, attempts to parse it as YAML,
    and returns the parsed contents as a dictionary. If there's an error in parsing the YAML file,
    it catches the exception, prints an error message, and returns an empty dictionary.

    Args:
        filepath (str): The path to the YAML configuration file to be read.

    Returns:
        Dict[str, Any]: A dictionary containing the contents of the YAML file.
                        Returns an empty dictionary if there's a parsing error.

    Raises:
        FileNotFoundError: If the file specified by filepath does not exist.
        IOError: If there is an error opening the file.
    """
    with open(filepath, 'r') as file:
        try:
            yaml_data = yaml.safe_load(file)
        except yaml.YAMLError as e:
            print(f"Error parsing yaml file: {e}")
            yaml_data = {}
    return yaml_data