import boto3
from botocore.client import Config
from botocore.exceptions import NoCredentialsError, ClientError, EndpointConnectionError
import logging
import os
import json
import mimetypes

CONFIG_FILE_PATH = '/data/config/s3_config.json'
boto3.set_stream_logger('botocore', level='INFO')  # Reduced from DEBUG for cleaner logs


def get_s3_client():
    """
    Initializes S3 client. Priority for credentials is:
    1. From a temporary config file (used by a UI).
    2. From environment variables.
    3. From hardcoded defaults for local development.
    """
    config = {}
    # Try to load from config file first
    if os.path.exists(CONFIG_FILE_PATH):
        try:
            with open(CONFIG_FILE_PATH, 'r') as f:
                content = f.read()
                if content:
                    config = json.loads(content)
        except (IOError, json.JSONDecodeError):
            pass  # Ignore errors and fall back

    s3_endpoint_url = config.get('S3_ENDPOINT_URL') or os.getenv('S3_ENDPOINT_URL') or 'http://localhost:19000'
    s3_access_key = config.get('S3_ACCESS_KEY') or os.getenv('S3_ACCESS_KEY') or 'anykey'
    s3_secret_key = config.get('S3_SECRET_KEY') or os.getenv('S3_SECRET_KEY') or 'anysecret'
    s3_region = config.get('S3_REGION') or os.getenv('S3_REGION') or 'us-east-1'

    if not all([s3_endpoint_url, s3_access_key, s3_secret_key]):
        return None, "S3 connection details could not be determined."

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name=s3_region,
            config=Config(
                s3={'addressing_style': 'path'},
                signature_version='s3v4',
                connect_timeout=5,
                retries={'max_attempts': 1}
            )
        )
        s3_client.list_buckets()
        return s3_client, None
    except EndpointConnectionError:
        return None, f"Could not connect to endpoint: {s3_endpoint_url}"
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'InvalidAccessKeyId':
            return None, "The Access Key ID is invalid."
        if error_code == 'SignatureDoesNotMatch':
            return None, "The Secret Access Key is incorrect."
        return None, f"An S3 client error occurred: {error_code}"
    except Exception as e:
        return None, f"An unexpected error occurred: {e}"


def list_buckets():
    s3, error = get_s3_client()
    if error:
        return None, error
    try:
        response = s3.list_buckets()
        return [bucket['Name'] for bucket in response['Buckets']], None
    except ClientError as e:
        return None, f"Could not list buckets. Error: {e}"


def list_objects(bucket_name, prefix=''):
    s3, error = get_s3_client()
    if error:
        return None, None, error
    folders, files = [], []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
        for page in pages:
            if 'CommonPrefixes' in page:
                for p in page['CommonPrefixes']:
                    folders.append(p['Prefix'])
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'] != prefix:
                        files.append(obj)
        return folders, files, None
    except ClientError as e:
        return None, None, f"Could not list objects. Error: {e}"


def upload_file(file_obj, bucket_name, object_name=None):
    """
    Uploads a file-like object to an S3-compatible bucket.
    Fixes:
      - Ensures file pointer is reset before upload.
      - Adds MIME type detection for better handling of .jsonl and text files.
      - Improved error logging.
    """
    s3, error = get_s3_client()
    if error:
        return False, error
    if object_name is None:
        object_name = file_obj.filename

    try:
        # Ensure file pointer is at start
        file_obj.seek(0)

        # Guess MIME type (e.g., .jsonl -> application/json)
        content_type = mimetypes.guess_type(object_name)[0] or 'application/octet-stream'

        # Upload with explicit content type
        s3.upload_fileobj(
            file_obj,
            bucket_name,
            object_name,
            ExtraArgs={'ContentType': content_type}
        )

        logging.info(f"✅ Uploaded '{object_name}' to bucket '{bucket_name}' ({content_type})")
        return True, None

    except ClientError as e:
        logging.error(f"Could not upload file '{object_name}': {e}", exc_info=True)
        return False, f"Could not upload file. Error: {e}"
    except Exception as e:
        logging.exception(f"Unexpected error uploading '{object_name}'")
        return False, str(e)


def download_file(bucket_name, object_name):
    s3, error = get_s3_client()
    if error:
        return None, error
    try:
        file_obj = s3.get_object(Bucket=bucket_name, Key=object_name)
        return file_obj['Body'], None
    except ClientError as e:
        return None, f"Could not download file. Error: {e}"


def delete_object(bucket_name, object_key):
    s3, error = get_s3_client()
    if error:
        return False, error
    try:
        s3.delete_object(Bucket=bucket_name, Key=object_key)
        return True, None
    except ClientError as e:
        return False, f"Could not delete object. Error: {e}"


def delete_folder(bucket_name, prefix):
    s3, error = get_s3_client()
    if error:
        return 0, error

    objects_to_delete = []
    try:
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})

        if not objects_to_delete:
            return 0, None

        for i in range(0, len(objects_to_delete), 1000):
            s3.delete_objects(
                Bucket=bucket_name,
                Delete={'Objects': objects_to_delete[i:i + 1000], 'Quiet': True}
            )

        return len(objects_to_delete), None
    except ClientError as e:
        return 0, f"Could not delete folder contents. Error: {e}"


def delete_bucket(bucket_name):
    s3, error = get_s3_client()
    if error:
        return False, error
    try:
        s3.delete_bucket(Bucket=bucket_name)
        return True, None
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketNotEmpty':
            return False, "Bucket is not empty and cannot be deleted. Please delete all contents first."
        return False, f"Could not delete bucket. Error: {e}"


def create_bucket(bucket_name):
    s3, error = get_s3_client()
    if error:
        return False, error
    try:
        s3.create_bucket(Bucket=bucket_name)
        return True, None
    except ClientError as e:
        return False, f"Could not create bucket. Error: {e}"
