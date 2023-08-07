"""Utils file to define all the utility functions."""
import logging
import boto3

from collections import namedtuple
from urllib.parse import urlparse

from constants import LOCAL_ENDPOINT_URL


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s"
)


def pull_file_from_s3(bucket, path, local_path, is_environment_local):
    """Pull file from S3 and store it to local path."""
    if is_environment_local:
        s3_client = boto3.client("s3", endpoint_url=LOCAL_ENDPOINT_URL)
    else:
        s3_client = boto3.client("s3")

    s3_client.download_file(bucket, path, local_path)
    logging.info(f"File from S3 = {path} downloaded to = {local_path}")
    return local_path


def resolve_s3_location(s3_path):
    """Resolve S3 path to bucket and file_key"""
    s3locobj = namedtuple("s3_location", ["bucket", "file_key"])
    s3_res = urlparse(s3_path)
    s3loc = s3locobj(s3_res.netloc, s3_res.path[1:] if s3_res.netloc else s3_res.path)

    return s3loc


def get_all_files_from_s3(bucket, prefix, is_environment_local):
    """Get all the files from S3 bucket matching the prefix."""
    filename_list = []
    if is_environment_local:
        s3_client = boto3.client("s3", endpoint_url=LOCAL_ENDPOINT_URL)
    else:
        s3_client = boto3.client(client="s3")

    logging.info(f"List all files from S3 = {bucket} with prefix = {prefix}")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for each_object in response["Contents"]:
        filename_list.append(each_object["Key"])

    return filename_list
