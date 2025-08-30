#!/usr/bin/env python3
"""
Pinnacle Tennis Odds Data Collection Pipeline
- Downloads tennis odds data from RapidAPI Pinnacle service
- Uploads to Oracle Cloud Object Storage
- Compresses local files and manages storage
- Sends email notifications on success/failure
- Uses Brazil (São Paulo) time for filenames and folder paths
"""
import os
import sys
import json
import requests
import zipfile
import traceback
import smtplib
import configparser
from datetime import datetime
from zoneinfo import ZoneInfo
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import oci
from oci.object_storage import ObjectStorageClient

# Brazil (São Paulo) timezone
BRAZIL_TZ = ZoneInfo("America/Sao_Paulo")

def load_config():
    """Load configuration from config.ini file (called once)"""
    config = configparser.ConfigParser()
    config_paths = [
        'config/config.ini',
        '../config/config.ini',
        '../../config/config.ini',
        './config.ini'
    ]
    config_file = next((p for p in config_paths if os.path.exists(p)), None)
    if not config_file:
        raise FileNotFoundError(
            "Configuration file not found. Please create config/config.ini with your credentials."
        )
    config.read(config_file)
    return config, config_file

def log_message(message, level="INFO", config=None):
    """Log message with timestamp"""
    timestamp = datetime.now(tz=BRAZIL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    entry = f"[{timestamp}] {level}: {message}"
    print(entry)
    if config:
        try:
            log_file = config.get('paths', 'log_file', fallback='/tmp/tennis_ingestion.log')
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            with open(log_file, 'a') as f:
                f.write(entry + '\n')
        except:
            pass

def send_email_notification(subject, body, success=True, config=None):
    """Send email notification about job status"""
    if not config:
        return
    try:
        msg = MIMEMultipart()
        msg["From"] = config.get('email', 'sender_email')
        msg["To"] = config.get('email', 'recipient_email')
        msg["Subject"] = f"{'✅ SUCCESS' if success else '❌ FAILURE'}: {subject}"
        full_body = f"""
Job Status: {'SUCCESS' if success else 'FAILURE'}
Timestamp: {datetime.now(tz=BRAZIL_TZ).strftime('%Y-%m-%d %H:%M:%S')}
Server: Oracle Cloud VM

Details:
{body}

---
This is an automated notification from your tennis data ingestion pipeline.
"""
        msg.attach(MIMEText(full_body, "plain"))
        server = smtplib.SMTP(config.get('email', 'smtp_server'),
                              config.getint('email', 'smtp_port'))
        server.starttls()
        server.login(config.get('email', 'sender_email'),
                     config.get('email', 'sender_password'))
        server.send_message(msg)
        server.quit()
        log_message("Email notification sent successfully", config=config)
    except Exception as e:
        log_message(f"Failed to send email: {e}", "ERROR", config=config)

def download_tennis_odds(config):
    """Download tennis odds data from RapidAPI Pinnacle service"""
    log_message("Starting tennis odds data download from RapidAPI", config=config)
    URL = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
    QUERY_STRING = {"sport_id": "2", "is_have_odds": "true"}
    HEADERS = {
        "X-RapidAPI-Key": config.get('api', 'rapidapi_key'),
        "X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
    }
    response = requests.get(URL, headers=HEADERS, params=QUERY_STRING, timeout=30)
    response.raise_for_status()
    events = response.json().get('events', [])
    if not events:
        raise Exception("No events data found in API response")
    now = datetime.now(tz=BRAZIL_TZ)
    ts = now.strftime('%Y-%m-%d-%H%M%S')
    filename = f"tennis_odds_{ts}.json"
    data_dir = config.get('paths', 'data_directory', fallback='/tmp/tennis_data')
    os.makedirs(data_dir, exist_ok=True)
    path = os.path.join(data_dir, filename)
    with open(path, 'w') as f:
        json.dump(events, f, indent=2)
    log_message(f"Downloaded {os.path.getsize(path)} bytes to {path}", config=config)
    return path, events

def upload_to_oracle_cloud(filepath, events, config):
    """Upload JSON data to Oracle Cloud Object Storage"""
    log_message("Starting upload to Oracle Cloud Object Storage", config=config)
    oci_cfg = oci.config.from_file(config.get('oci', 'config_path'),
                                   config.get('oci', 'profile'))
    client = ObjectStorageClient(oci_cfg)
    now = datetime.now(tz=BRAZIL_TZ)
    folder = now.strftime('%Y/%m/%d')
    name = os.path.basename(filepath)
    obj = f"tennis_odds/{folder}/{name}"
    with open(filepath, 'rb') as f:
        client.put_object(
            namespace_name=config.get('oci', 'namespace'),
            bucket_name=config.get('oci', 'bucket_name'),
            object_name=obj,
            put_object_body=f
        )
    log_message(f"Uploaded to Oracle Cloud as {obj}", config=config)
    return obj

def compress_and_cleanup(filepath, config):
    """Compress the JSON file to ZIP and delete the original"""
    log_message("Compressing file and cleaning up", config=config)
    zip_path = filepath.replace('.json', '.zip')
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.write(filepath, os.path.basename(filepath))
    os.remove(filepath)
    log_message(f"Created {zip_path} ({os.path.getsize(zip_path)} bytes)", config=config)
    return zip_path

def main():
    start = datetime.now(tz=BRAZIL_TZ)
    try:
        config, cfg_path = load_config()
        log_message("=== Starting Tennis Odds Ingestion Pipeline ===", config=config)
        log_message(f"Configuration loaded from: {cfg_path}", config=config)
        path, events = download_tennis_odds(config)
        obj = upload_to_oracle_cloud(path, events, config)
        zip_path = compress_and_cleanup(path, config)
        duration = (datetime.now(tz=BRAZIL_TZ) - start).total_seconds()
        msg = f"""
Pipeline completed in {duration:.2f}s
Events: {len(events)}
Compressed file: {os.path.basename(zip_path)}
Cloud object: {obj}
"""
        log_message("Pipeline completed successfully", config=config)
        send_email_notification("Tennis Odds Data Pipeline", msg, True, config)
    except Exception as e:
        duration = (datetime.now(tz=BRAZIL_TZ) - start).total_seconds()
        err = f"Pipeline failed after {duration:.2f}s\nError: {e}\n\n{traceback.format_exc()}"
        log_message(err, "ERROR", config if 'config' in locals() else None)
        send_email_notification("Tennis Odds Data Pipeline", err, False,
                                config if 'config' in locals() else None)
        sys.exit(1)

if __name__ == "__main__":
    main()
