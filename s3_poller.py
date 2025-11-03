import os
import time
import json
import requests
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import logging

# --- Configuration Files ---
# Shared with the Flask app
S3_CONFIG_FILE = '/tmp/s3_config.json'
# Specific to the poller
POLLING_CONFIG_FILE = '/tmp/polling_config.json'

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Poller] - %(levelname)s - %(message)s')

def get_s3_client():
    """Initializes S3 client using the shared config file."""
    if not os.path.exists(S3_CONFIG_FILE):
        logging.warning("S3 config file not found. Waiting for it to be created by the UI...")
        return None

    try:
        with open(S3_CONFIG_FILE, 'r') as f:
            config = json.load(f)
    except (IOError, json.JSONDecodeError):
        logging.error("Could not read or parse S3 config file.")
        return None

    s3_endpoint_url = config.get('S3_ENDPOINT_URL')
    s3_access_key = config.get('S3_ACCESS_KEY')
    s3_secret_key = config.get('S3_SECRET_KEY')
    s3_region = config.get('S3_REGION')

    if not all([s3_endpoint_url, s3_access_key, s3_secret_key]):
        logging.error("S3 connection details are incomplete in the config file.")
        return None

    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
            region_name=s3_region,
            config=Config(signature_version='s3v4', retries={'max_attempts': 2})
        )
        s3_client.list_buckets()
        logging.info("✅ Successfully connected to S3 endpoint.")
        return s3_client
    except Exception as e:
        logging.error(f"❌ Could not connect to S3. Error: {e}")
        return None

def get_bucket_state(s3_client, bucket_name):
    """Scans a bucket and returns a dictionary of {object_key: ETag}."""
    state = {}
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name)
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    state[obj['Key']] = obj['ETag']
    except ClientError as e:
        logging.error(f"Error listing objects in bucket '{bucket_name}': {e}")
    return state

def send_notification(webhook_url, event_type, bucket_name, object_key):
    """Sends a notification payload to the configured webhook URL."""
    payload = {
        'event_type': event_type,
        'bucket': bucket_name,
        'object_key': object_key,
        'timestamp': time.time()
    }
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logging.info(f"🚀 Sent notification for {event_type}: {object_key} to {webhook_url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send webhook notification for {object_key}. Error: {e}")

def main_polling_loop():
    """The main loop that polls all configured buckets."""
    s3_client = None
    known_states = {} # e.g., {'bucket-one': {'file1.txt': 'etag...'}, 'bucket-two': {}}
    
    # Wait for the initial S3 configuration to be created by the web UI
    while s3_client is None:
        s3_client = get_s3_client()
        if s3_client is None:
            time.sleep(10)

    logging.info("Starting main polling loop...")
    while True:
        try:
            # Load the polling configuration on each iteration to get updates from the UI
            if not os.path.exists(POLLING_CONFIG_FILE):
                time.sleep(10)
                continue
            
            with open(POLLING_CONFIG_FILE, 'r') as f:
                polling_config = json.load(f)

            for bucket_name, config in polling_config.items():
                if not config.get('enabled'):
                    if bucket_name in known_states:
                        del known_states[bucket_name] # Remove disabled buckets from tracking
                    continue

                webhook_url = config['webhook_url']
                poll_interval = config['poll_interval']
                
                # If we see a new bucket, initialize its state
                if bucket_name not in known_states:
                    known_states[bucket_name] = get_bucket_state(s3_client, bucket_name)
                    logging.info(f"Now monitoring bucket '{bucket_name}'. Initial state has {len(known_states[bucket_name])} objects.")
                    continue # Start comparisons on the next cycle

                current_state = get_bucket_state(s3_client, bucket_name)
                previous_state = known_states[bucket_name]

                # --- Detect Changes ---
                for key, etag in current_state.items():
                    if key not in previous_state or previous_state[key] != etag:
                        logging.info(f"Change detected in '{bucket_name}': {key} was created or modified.")
                        send_notification(webhook_url, 'OBJECT_CREATED', bucket_name, key)

                for key in previous_state:
                    if key not in current_state:
                        logging.info(f"Change detected in '{bucket_name}': {key} was deleted.")
                        send_notification(webhook_url, 'OBJECT_DELETED', bucket_name, key)
                
                known_states[bucket_name] = current_state
            
            # Use a general poll interval after checking all buckets
            # A more advanced version could have per-bucket sleep times
            time.sleep(10) 

        except (IOError, json.JSONDecodeError):
            logging.warning("Polling config file not found or is invalid. Retrying...")
            time.sleep(15)
        except Exception as e:
            logging.error(f"An error occurred in the polling loop: {e}")
            time.sleep(60) # Longer sleep on unexpected error

if __name__ == "__main__":
    main_polling_loop()