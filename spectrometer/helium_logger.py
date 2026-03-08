"""
================================================================================
HELIUM & CRYOGEN LEVEL MONITOR
================================================================================
Description:    Monitors Bruker TopSpin helium logs to track cryogen levels
                and magnet field stability. It calculates liquid volumes
                and exports the data to InfluxDB for long-term trending.

Functionality:  - Scans local/Docker directories for 'heliumlog' files.
                - Parses timestamps, Helium %, Nitrogen %, and Field values.
                - Converts percentage levels to absolute liters based on
                  magnet-specific volume configurations.
                - Implements state tracking to prevent duplicate log entries.
                - Handles initial bulk-loading of historical logs on startup.

Environment:    Requires .env for InfluxDB API tokens, bucket names, and
                spectrometer-specific metadata (Volume, Room, Owner).
Author:         Thomas Kress aided by ChatGPT
Date:           March 2025
================================================================================
"""

import os
import glob
import time
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
from datetime import datetime, timezone
import re

# Load environment variables
load_dotenv()

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("HELIUM_INFLUXDB_BUCKET")
SPECTROMETER_NAME = os.getenv("SPECTROMETER_NAME")
ROOM_NAME= os.getenv("ROOM_NAME")
MANAGEMENT_NAME= os.getenv("MANAGEMENT_NAME")
OWNER_NAME= os.getenv("OWNER_NAME")

HELIUM_LOG_DIR_DOCKER = os.getenv("HELIUM_LOG_DIR", "/app/opt/")  # Default directory if not set
# HELIUM_LOG_DIR_DOCKER = "/opt/topspin*/prog/logfiles/"
HELIUM_LOG_FILE_NAME = os.getenv("HELIUM_LOG_FILE_NAME","heliumlog")
INTERVAL_SECONDS = int(os.getenv("HELIUM_INTERVAL_SECONDS", 3600))
HELIUM_VOLUME_LITERS = float(os.getenv("HELIUM_VOLUME_LITERS"))

# Initialize InfluxDB Client
client = influxdb_client.InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Track last logged timestamp
last_logged_timestamp = None

def find_latest_log():
    """Finds the most recent heliumlog file."""
    log_files = glob.glob(f"{HELIUM_LOG_DIR_DOCKER}{HELIUM_LOG_FILE_NAME}", recursive=True)
    if not log_files:
        print(f"❌ No log files found in {HELIUM_LOG_DIR_DOCKER}{HELIUM_LOG_FILE_NAME}")
        return None
    return max(log_files, key=os.path.getmtime)

def parse_log_entry(line):
    """Parses a single log entry and returns a dictionary of extracted values."""
    timestamp_match = re.match(r"^(.+?) :", line)
    helium_match = re.search(r"helium level = +([\d.]+) %", line)
    nitrogen_match = re.search(r"nitrogen level = +([\d.]+) %", line)
    field_match = re.search(r"field = +([\d.-]+)", line)

    if not timestamp_match or not helium_match or not field_match:
        print(f"⚠️ Skipping invalid log entry: {line.strip()}")
        return None

    timestamp_str = timestamp_match.group(1)
    try:
        timestamp = datetime.strptime(timestamp_str, "%a %b %d %H:%M:%S %Y")
    except ValueError:
        print(f"⚠️ Skipping entry with invalid timestamp: {timestamp_str}")
        return None

    return {
        "timestamp": timestamp,
        "helium_level": float(helium_match.group(1)),
        "nitrogen_level": float(nitrogen_match.group(1)) if nitrogen_match else None,
        "field": float(field_match.group(1)),
        "helium_level_liters": float(helium_match.group(1))/100*HELIUM_VOLUME_LITERS
    }

def process_logs(parse_all=False):
    """Parses and logs helium data from the latest log file."""
    global last_logged_timestamp

    log_file = find_latest_log()
    if not log_file:
        return

    with open(log_file, "r", errors="replace") as file:
        lines = file.readlines()
        if not lines:
            print("⚠️ Log file is empty.")
            return

    # Select all lines or only the latest
    lines_to_process = lines if parse_all else [line for line in reversed(lines) if line.strip()][0:1]

    for line in lines_to_process:
        log_data = parse_log_entry(line)
        if not log_data:
            continue

        # Avoid duplicate logging
        if last_logged_timestamp and log_data["timestamp"] <= last_logged_timestamp:
            print(f"⏳ No new data to log. Last logged: {last_logged_timestamp}")
            continue

        # Update last logged timestamp
        last_logged_timestamp = log_data["timestamp"]

        # Send to InfluxDB
        log_to_influxdb(log_data)

def log_to_influxdb(log_data):
    timestamp = log_data.pop("timestamp")
    timestamp = timestamp.replace(tzinfo=timezone.utc)  # Explicitly set to UTC

    # Convert to nanoseconds (InfluxDB default)
    point = influxdb_client.Point("bsms")\
        .tag("spectrometer", SPECTROMETER_NAME) \
        .tag("management", MANAGEMENT_NAME) \
        .tag("owner", OWNER_NAME) \
        .tag("room", ROOM_NAME) \
        .time(int(timestamp.timestamp() * 1_000_000_000))  # Convert to nanoseconds

    for key, value in log_data.items():
        if value is not None:
            point = point.field(key, value)

    write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
    print(f"✅ Logged to InfluxDB: {log_data}, Timestamp: {timestamp.isoformat()}Z")

if __name__ == "__main__":
    process_logs(parse_all=True)  # Process all logs initially
    while True:
        process_logs(parse_all=False)  # Then, only process new logs
        time.sleep(INTERVAL_SECONDS)