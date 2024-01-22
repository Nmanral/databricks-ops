import os
import json
from typing import Optional

from workflow.json_utils import read_json_file_from_s3, list_all_objects_in_s3_path


def generate_version(current_version: str) -> str:
    """
    Generates the next minor version based on the current version.

    Args:
        current_version (str): The current version in the format 'major.minor'.

    Returns:
        str: The next minor version in the format 'major.minor'. 
             If `current_version` is None or empty, returns '1.0'.
    """
    if current_version:
        major, minor = map(int, current_version.split('.'))
        if minor == 9:
            major += 1
            minor = 0
        else:
            minor += 1
        return f"{major}.{minor}"
    else:
        return "1.0"


def get_current_version(bucket_name: str, file_path: str) -> Optional[str]:
    """
    Retrieves the current version from a specified file in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        file_path (str): The path to the file within the S3 bucket.

    Returns:
        Optional[str]: The current version as a string if found, otherwise None.

    Raises:
        FileNotFoundError: If the specified file does not exist in the S3 bucket.
        json.JSONDecodeError: If the file content is not valid JSON.
        KeyError: If the expected 'metadata' or 'version' keys are not found in the JSON.
    """
    try:
        data = read_json_file_from_s3(bucket_name, file_path)
        if data is None:
            version = data["metadata"]["version"]
            return version
        else:
            pass
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        return None


def get_previous_version_file(bucket_name: str, historic_state_path: str) -> Optional[str]:
    """
    Retrieves the file name of the previous version from a specified path in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        historic_state_path (str): The path within the S3 bucket where version files are stored.

    Returns:
        Optional[str]: The file name of the previous version if found, otherwise None.

    Note:
        Files are expected to follow the naming convention 'state_file_<version>.json', 
        where <version> is an integer. This function retrieves the file with the second-highest
        version number. If only one file is present, it returns that file.
    """
    files = list_all_objects_in_s3_path(bucket_name, historic_state_path)
    files_with_version = [(f, int(f.split("_")[-1].split(".")[0])) for f in files if f.startswith('sate_file_') and f.endswith('.json')]
    files_with_version.sort(key=lambda x: x[1])

    if len(files_with_version) > 1:
        index_to_retrieve = -1 if len(files_with_version) == 1 else -2
        previous_version_file = os.path.join(historic_state_path, files_with_version[index_to_retrieve][0])
        return previous_version_file
    else:
        return None