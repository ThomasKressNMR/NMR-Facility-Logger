"""
================================================================================
LOCK DRIFT LOGGER
================================================================================
Description:    Fetches spectrometer lock drift data from a remote URL,
                parses the magnetic field values, and pushes time-series
                data to InfluxDB. Better time resolution than in helium_logger.py

Functionality:  - Periodically polls a log file via HTTP.
                - Uses regex to extract sequence numbers and field strengths.
                - Normalizes timestamps based on sequence "time numbers."
                - Stores metadata (Spectrometer, Room, Owner) as InfluxDB tags.

Environment:    Requires a .env file with InfluxDB credentials and target URLs.
Author:         Thomas Kress aided by ChatGPT
Date:           March 2025
================================================================================
"""


import os
import re
import time
import requests
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv



# Load environment variables
load_dotenv()

# ====== InfluxDB Configuration ======
INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("FIELD_INFLUXDB_BUCKET", "Field")
SPECTROMETER_NAME = os.getenv("SPECTROMETER_NAME")
ROOM_NAME = os.getenv("ROOM_NAME")
MANAGEMENT_NAME = os.getenv("MANAGEMENT_NAME")
OWNER_NAME = os.getenv("OWNER_NAME")

# ====== Script Configuration ======
LOCK_DRIFT_URL = os.getenv("LOCK_DRIFT_URL")
INTERVAL_SECONDS = int(os.getenv("DRIFT_LOG_INTERVAL_SECONDS", 300))  # Default: every 5 minutes

# ====== InfluxDB Setup ======
client = influxdb_client.InfluxDBClient(
    url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Track last logged "time number" to avoid duplicates
last_logged_number = None

def fetch_lock_drift_data():
    try:
        response = requests.get(LOCK_DRIFT_URL)
        response.raise_for_status()
        return response.text.splitlines()
    except Exception as e:
        print(f"❌ Failed to fetch Lock Drift data: {e}")
        return []

def parse_all_entries(lines):
    """
    Parse all lines matching pattern.
    Returns list of dicts: [{"num": int, "field": float}, ...]
    """
    pattern = re.compile(r"^(\d+):.*?Field\s*=\s*([\d.]+),.*Auto\s*-\s*Locked")
    entries = []
    for line in lines:
        match = pattern.search(line)
        if match:
            num = int(match.group(1))
            field = float(match.group(2))
            entries.append({"num": num, "field": field})
    return entries

def log_to_influxdb(data_list, current_time):
    max_num = max(d["num"] for d in data_list)

    for data in data_list:
        num = data["num"]

        # Calculate offset in seconds assuming 1 unit = 1 second
        offset_seconds = num - max_num  # negative or zero
        timestamp = current_time + timedelta(seconds=offset_seconds)

        point = influxdb_client.Point("field_drift") \
            .tag("spectrometer", SPECTROMETER_NAME) \
            .tag("management", MANAGEMENT_NAME) \
            .tag("owner", OWNER_NAME) \
            .tag("room", ROOM_NAME) \
            .field("field", data["field"]) \
            .time(timestamp)

        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        print(f"✅ Logged field = {data['field']} at {timestamp.isoformat()}Z")

def main():
    print("🚀 Starting Lock Drift Logger...")

    while True:
        lines = fetch_lock_drift_data()
        if not lines:
            print("⚠️ No data fetched, retrying...")
            time.sleep(INTERVAL_SECONDS)
            continue

        entries = parse_all_entries(lines)
        if not entries:
            print("⚠️ No valid entries found in data")
            time.sleep(INTERVAL_SECONDS)
            continue

        current_time = datetime.now(timezone.utc).replace(microsecond=0)  # round to nearest second
        log_to_influxdb(entries, current_time)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
