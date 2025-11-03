from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import s3_utils
import os
import json
from werkzeug.utils import secure_filename # We still import it, but use it differently
import threading
import time
import logging
import requests
from botocore.exceptions import ClientError

# --- Global Configuration ---
app = Flask(__name__)
app.secret_key = os.urandom(24)
CONFIG_DIR = '/data/config'
POLLING_CONFIG_FILE = os.path.join(CONFIG_DIR, 'polling_config.json')
S3_CONFIG_FILE = os.path.join(CONFIG_DIR, 's3_config.json')
LOG_HEARTBEAT_INTERVAL = 30 # Number of loops before logging a heartbeat (30 loops * 10s = 5 mins)

# --- Polling Logic (Now part of the main app) ---
# Set logging level for the poller
poller_logger = logging.getLogger("PollingThread")
poller_logger.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(name)s] - %(levelname)s - %(message)s')

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
        poller_logger.error(f"Error listing objects in bucket '{bucket_name}': {e}")
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
        # This is now the primary log for detected changes
        poller_logger.info(f"🚀 Sent notification for {event_type}: {object_key} to {webhook_url}")
    except requests.exceptions.RequestException as e:
        poller_logger.error(f"Failed to send webhook notification for {object_key}. Error: {e}")

def poller_background_thread():
    """The main loop that polls all configured buckets."""
    s3_client = None
    known_states = {}
    loop_count = 0
    
    poller_logger.info("Polling thread started. Waiting for S3 configuration...")
    
    while s3_client is None:
        s3_client, _ = s3_utils.get_s3_client()
        if s3_client is None:
            time.sleep(10) # Wait for the UI to be configured

    poller_logger.info("S3 client initialized in poller. Starting main polling loop...")
    while True:
        try:
            loop_count += 1
            if not os.path.exists(POLLING_CONFIG_FILE):
                time.sleep(10)
                continue
            
            with open(POLLING_CONFIG_FILE, 'r') as f:
                polling_config = json.load(f)

            active_buckets = {b for b, c in polling_config.items() if c.get('enabled')}
            
            if loop_count % LOG_HEARTBEAT_INTERVAL == 0:
                poller_logger.info(f"Polling {len(active_buckets)} active bucket(s). Heartbeat...")
                loop_count = 0 # Reset counter

            for bucket_name in list(known_states.keys()):
                if bucket_name not in active_buckets:
                    del known_states[bucket_name]
                    poller_logger.info(f"Stopped monitoring bucket '{bucket_name}'.")

            for bucket_name, config in polling_config.items():
                if not config.get('enabled'):
                    continue

                webhook_url = config['webhook_url']
                
                if bucket_name not in known_states:
                    known_states[bucket_name] = get_bucket_state(s3_client, bucket_name)
                    poller_logger.info(f"Now monitoring bucket '{bucket_name}'. Initial state has {len(known_states[bucket_name])} objects.")
                    continue

                current_state = get_bucket_state(s3_client, bucket_name)
                previous_state = known_states[bucket_name]

                # Compare states and send notifications (logging is now in send_notification)
                for key, etag in current_state.items():
                    if key not in previous_state or previous_state[key] != etag:
                        send_notification(webhook_url, 'OBJECT_CREATED', bucket_name, key)

                for key in previous_state:
                    if key not in current_state:
                        send_notification(webhook_url, 'OBJECT_DELETED', bucket_name, key)
                
                known_states[bucket_name] = current_state
            
            time.sleep(10)

        except (IOError, json.JSONDecodeError):
            poller_logger.warning("Polling config file not found or is invalid. Retrying...")
            time.sleep(15)
        except Exception as e:
            poller_logger.error(f"An error occurred in the polling loop: {e}", exc_info=True)
            time.sleep(60)

# --- Jinja2 Filter for KB ---
@app.template_filter('kb_format')
def kb_format(value):
    """Converts bytes to a formatted KB string."""
    try:
        kb = int(value) / 1024
        if kb < 0.1:
            return f"{kb:.3f} KB"
        return f"{kb:,.2f} KB"
    except (ValueError, TypeError):
        return "--"

# --- Flask Routes ---
def load_polling_config():
    """Loads the polling configuration from its JSON file."""
    if not os.path.exists(POLLING_CONFIG_FILE):
        return {}
    try:
        with open(POLLING_CONFIG_FILE, 'r') as f:
            return json.load(f)
    except (IOError, json.JSONDecodeError):
        return {}

def save_polling_config(config):
    """Saves the polling configuration to its JSON file."""
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        with open(POLLING_CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        return True, None
    except IOError as e:
        return False, str(e)

@app.route('/')
def index():
    buckets, error = s3_utils.list_buckets()
    if error:
        flash(f"Could not list buckets. Please check your configuration. Error: {error}", "warning")
        return redirect(url_for('configure'))
    return render_template('index.html', buckets=buckets)

@app.route('/bucket/<bucket_name>')
def view_bucket(bucket_name):
    prefix = request.args.get('prefix', '')
    folders, files, error = s3_utils.list_objects(bucket_name, prefix)
    if error:
        return render_template('error.html', error_message=error)

    breadcrumbs = [{'name': 'Buckets', 'url': url_for('index')}]
    breadcrumbs.append({'name': bucket_name, 'url': url_for('view_bucket', bucket_name=bucket_name)})
    if prefix:
        path_parts = prefix.strip('/').split('/')
        current_path = ''
        for part in path_parts:
            current_path += f"{part}/"
            breadcrumbs.append({'name': part, 'url': url_for('view_bucket', bucket_name=bucket_name, prefix=current_path)})

    return render_template('bucket.html', 
                           bucket_name=bucket_name, prefix=prefix,
                           folders=folders, files=files, breadcrumbs=breadcrumbs)

@app.route('/upload/<bucket_name>', methods=['POST'])
def upload(bucket_name):
    prefix = request.form.get('prefix', '')
    files = request.files.getlist('files[]')

    if not files or files[0].filename == '':
        flash('No files were selected for upload.', 'warning')
        return redirect(url_for('view_bucket', bucket_name=bucket_name, prefix=prefix))
        
    success_count = 0
    error_messages = []

    for file in files:
        original_path = file.filename
        
        if file.content_length == 0 and file.content_type == 'application/octet-stream':
            continue
            
        # --- NEW ROBUST FIX ---
        # We will not use secure_filename, as it is too restrictive for S3 object keys.
        # Instead, we will manually clean the path to prevent traversal and whitespace issues.
        
        # S3 keys use forward slashes, but the browser might send backslashes
        normalized_path = original_path.replace("\\", "/")
        
        parts = normalized_path.split('/')
        clean_parts = []
        
        is_path_valid = True
        for part in parts:
            # 1. Clean whitespace from beginning and end
            clean_part = part.strip()
            
            # 2. Check for invalid/traversal parts
            if not clean_part or clean_part == '.' or clean_part == '..':
                is_path_valid = False
                break
            
            clean_parts.append(clean_part)
            
        if not is_path_valid or not clean_parts:
            error_messages.append(f"Skipped file with invalid path: '{original_path}'")
            continue
            
        # Re-join the path with the S3 separator
        clean_path = "/".join(clean_parts)
        # --- END OF NEW FIX ---

        object_name = f"{prefix}{clean_path}"
        
        success, error = s3_utils.upload_file(file, bucket_name, object_name)
        if success:
            success_count += 1
        else:
            error_messages.append(f"Failed to upload '{clean_path}': {error}")

    if success_count > 0:
        flash(f'Successfully uploaded {success_count} file(s).', 'success')
    if error_messages:
        # Join multiple errors with a <br> for better display
        flash('<br>'.join(error_messages), 'danger')

    return redirect(url_for('view_bucket', bucket_name=bucket_name, prefix=prefix))


@app.route('/download/<bucket_name>/<path:object_name>')
def download(bucket_name, object_name):
    file_obj, error = s3_utils.download_file(bucket_name, object_name)
    if error:
        return render_template('error.html', error_message=error)
    download_name = object_name.split('/')[-1]
    return send_file(file_obj, as_attachment=True, download_name=download_name)

@app.route('/delete_selected/<bucket_name>', methods=['POST'])
def delete_selected(bucket_name):
    items_to_delete = request.form.getlist('selected_items')
    prefix = request.form.get('prefix', '')
    deleted_count = 0
    error_messages = []

    for item_key in items_to_delete:
        if item_key.endswith('/'):
            count, error = s3_utils.delete_folder(bucket_name, item_key)
            if error: error_messages.append(f"Failed to delete folder '{item_key}': {error}")
            else: deleted_count += count
        else:
            success, error = s3_utils.delete_object(bucket_name, item_key)
            if error: error_messages.append(f"Failed to delete file '{item_key}': {error}")
            else: deleted_count += 1
    
    if deleted_count > 0: flash(f'Successfully deleted {deleted_count} object(s).', 'success')
    if error_messages: flash('<br>'.join(error_messages), 'danger')

    return redirect(url_for('view_bucket', bucket_name=bucket_name, prefix=prefix))

@app.route('/delete_bucket/<bucket_name>', methods=['POST'])
def delete_bucket(bucket_name):
    success, error = s3_utils.delete_bucket(bucket_name)
    flash(f'Bucket "{bucket_name}" deleted.' if success else f'Error: {error}', 'success' if success else 'danger')
    return redirect(url_for('index'))

@app.route('/create_bucket', methods=['POST'])
def create_bucket():
    bucket_name = request.form.get('bucket_name')
    if not bucket_name:
        flash('Bucket name cannot be empty.', 'warning')
        return redirect(url_for('index'))
    
    success, error = s3_utils.create_bucket(bucket_name)
    flash(f'Bucket "{bucket_name}" created successfully.' if success else f'Error creating bucket: {error}', 'success' if success else 'danger')
    return redirect(url_for('index'))

@app.route('/notifications/<bucket_name>', methods=['GET', 'POST'])
def configure_notifications(bucket_name):
    polling_config = load_polling_config()

    if request.method == 'POST':
        action = request.form.get('action')
        
        if action == 'disable':
            if bucket_name in polling_config:
                polling_config[bucket_name]['enabled'] = False
                success, error = save_polling_config(polling_config)
                flash('Polling disabled for this bucket.' if success else f'Error: {error}', 'success' if success else 'danger')
        else:
            webhook_url = request.form.get('webhook_url')
            
            if not webhook_url:
                flash('Webhook URL cannot be empty.', 'warning')
            else:
                polling_config[bucket_name] = {
                    'enabled': True,
                    'webhook_url': webhook_url
                }
                success, error = save_polling_config(polling_config)
                flash('Polling configuration saved.' if success else f'Error: {error}', 'success' if success else 'danger')

        return redirect(url_for('configure_notifications', bucket_name=bucket_name))

    current_config = polling_config.get(bucket_name, {})
    return render_template('notifications.html', bucket_name=bucket_name, config=current_config)

@app.route('/configure', methods=['GET', 'POST'])
def configure():
    if request.method == 'POST':
        new_config = {
            'S3_ENDPOINT_URL': request.form['endpoint_url'],
            'S3_ACCESS_KEY': request.form['access_key'],
            'S3_SECRET_KEY': request.form['secret_key'],
            'S3_REGION': request.form['region']
        }
        try:
            os.makedirs(CONFIG_DIR, exist_ok=True)
            with open(S3_CONFIG_FILE, 'w') as f:
                json.dump(new_config, f, indent=4)
            flash('Configuration saved. The application will now use the new settings.', 'info')
            return redirect(url_for('configure'))
        except IOError as e:
            flash(f'Could not save configuration file: {e}', 'danger')

    current_config = {}
    if os.path.exists(S3_CONFIG_FILE):
        try:
            with open(S3_CONFIG_FILE, 'r') as f:
                content = f.read()
                if content: current_config = json.loads(content)
        except (IOError, json.JSONDecodeError):
            flash('Could not read saved configuration file.', 'warning')
    
    current_config.setdefault('S3_ENDPOINT_URL', os.getenv('S3_ENDPOINT_URL', ''))
    current_config.setdefault('S3_ACCESS_KEY', os.getenv('S3_ACCESS_KEY', ''))
    
    _, error = s3_utils.get_s3_client()
    connection_status = "Successfully Connected" if not error else "Connection Failed"
    
    return render_template('configure.html',
                           config=current_config,
                           connection_status=connection_status,
                           connection_error=error)

if __name__ == '__main__':
    # Set the logging level for the main Flask app (werkzeug) to WARNING
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    
    poller_thread = threading.Thread(target=poller_background_thread, name="PollingThread", daemon=True)
    poller_thread.start()
    
    app.run(host='0.0.0.0', port=5001)