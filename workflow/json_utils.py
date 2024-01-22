import boto3
import json
from typing import List, Optional


def write_json_to_s3(data: dict, bucket_name: str, s3_key: str) -> bool:
    """
    Writes a given dictionary as a JSON file to an S3 bucket.

    This function takes a dictionary, converts it into a JSON formatted string,
    and then uploads it to the specified AWS S3 bucket with the provided S3 key.
    
    Args:
        data (dict): The data to be written to the S3 bucket. This should be a
            dictionary that can be serialized into a JSON format.
        bucket_name (str): The name of the S3 bucket where the JSON file is to be
            stored.
        s3_key (str): The S3 key to be used for the JSON file. This is essentially
            the path and file name under which the file will be stored in the S3 bucket.

    Returns:
        bool: True if the file was written successfully, False otherwise.

    Raises:
        ValueError: If the `bucket_name` or `s3_key` is empty.
    """
    if not bucket_name or s3_key:
        raise ValueError("bucket name or s3 key must not be empty.")
    
    s3 = boto3.client('s3')
    try:
        content = json.dumps(data, indent=4)
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=content)
        return True
    except Exception as e:
        print(f"Error writing json data to s3: {e}")
        return False
    

def read_json_file_from_s3(bucket_name: str, s3_key: str) -> Optional[dict]:
    """
    Reads a JSON file from an S3 bucket and returns its content.

    This function retrieves a specified JSON file from an AWS S3 bucket and
    returns its contents as a dictionary. If the file cannot be read, it 
    returns None.

    Args:
        bucket_name (str): The name of the S3 bucket from which to read.
        s3_key (str): The S3 key for the file to be read.

    Returns:
        Optional[dict]: A dictionary representation of the JSON file's contents,
        or None if the file could not be read or is not a valid JSON.

    Raises:
        ValueError: If the `bucket_name` or `s3_key` is empty.
    """
    if not bucket_name or not s3_key:
        raise ValueError("bucket name or s3 key must not be empty.")
    
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        print(f"Error reading json data from s3: {e}")
        pass


def list_all_objects_in_s3_path(bucket_name: str, s3_path: str) -> List[str]:
    """
    Lists all objects in a specified path within an S3 bucket.

    This function returns a list of filenames for all objects stored in a
    specified path within the given S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        s3_path (str): The path within the S3 bucket from which to list objects.
            This path should not include the bucket name.

    Returns:
        List[str]: A list of filenames (strings) of all objects in the specified
        S3 path.

    Raises:
        ValueError: If the `bucket_name` or `s3_path` is empty.
    """
    if not bucket_name or not s3_path:
        raise ValueError("bucket name or s3 path must not be empty.")
    
    s3 = boto3.client('s3')
    files = []

    if not s3_path.endswith('/'):
        s3_path += '/'
    
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket_name, Prefix=s3_path):
        if 'Contents' in page:
            for obj in page['Contents']:
                file_path = obj['Key']
                filename = file_path.split('/')[-1]
                files.append(filename)
    
    return files