#!/usr/bin/env python3
"""
Pinnacle Tennis Odds Data Collection Pipeline
- Downloads tennis odds data from RapidAPI Pinnacle service
- Uploads to Oracle Cloud Object Storage
- Sends email notifications on success/failure
- Uses Brazil (São Paulo) time for filenames and folder paths
"""
import os
import sys
import json
import requests
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

def download_and_upload_tennis_odds(config):
    """Download tennis odds data and upload directly to Oracle Cloud Object Storage"""
    log_message("Starting tennis odds data download from RapidAPI", config=config)
    
    # API configuration
    URL = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
    QUERY_STRING = {"sport_id": "2", "is_have_odds": "true"}
    HEADERS = {
        "X-RapidAPI-Key": config.get('api', 'rapidapi_key'),
        "X-RapidAPI-Host": "pinnacle-odds.p.rapidapi.com"
    }
    
    # Download data
    response = requests.get(URL, headers=HEADERS, params=QUERY_STRING, timeout=30)
    response.raise_for_status()
    events = response.json().get('events', [])
    
    if not events:
        raise Exception("No events data found in API response")
    
    log_message(f"Downloaded {len(events)} events from API", config=config)
    
    # Generate filename and object path
    now = datetime.now(tz=BRAZIL_TZ)
    ts = now.strftime('%Y-%m-%d-%H%M%S')
    filename = f"tennis_odds_{ts}.json"
    folder = now.strftime('%Y/%m/%d')
    object_name = f"tennis_odds/{folder}/{filename}"
    
    # Convert events to JSON bytes
    json_data = json.dumps(events, indent=2)
    json_bytes = json_data.encode('utf-8')
    
    # Upload directly to Oracle Cloud
    log_message("Starting upload to Oracle Cloud Object Storage", config=config)
    oci_cfg = oci.config.from_file(config.get('oci', 'config_path'),
                                   config.get('oci', 'profile'))
    client = ObjectStorageClient(oci_cfg)
    
    client.put_object(
        namespace_name=config.get('oci', 'namespace'),
        bucket_name=config.get('oci', 'bucket_name'),
        object_name=object_name,
        put_object_body=json_bytes
    )
    
    log_message(f"Uploaded {len(json_bytes)} bytes to Oracle Cloud as {object_name}", config=config)
    return object_name, events

def main():
    start = datetime.now(tz=BRAZIL_TZ)
    try:
        config, cfg_path = load_config()
        log_message("=== Starting Tennis Odds Ingestion Pipeline ===", config=config)
        log_message(f"Configuration loaded from: {cfg_path}", config=config)
        
        # Download and upload in one step
        object_name, events = download_and_upload_tennis_odds(config)
        
        duration = (datetime.now(tz=BRAZIL_TZ) - start).total_seconds()
        msg = f"""
Pipeline completed successfully in {duration:.2f} seconds

Data Summary:
- Events processed: {len(events)}
- Oracle Cloud location: {object_name}
- Data size: {len(json.dumps(events, indent=2).encode('utf-8'))} bytes

Storage Strategy:
- Direct upload to Oracle Cloud Object Storage
- No local file storage (cloud-only approach)
- JSON format preserved for analysis

VM Activity: Regular execution maintains Oracle Cloud VM activity status.
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
