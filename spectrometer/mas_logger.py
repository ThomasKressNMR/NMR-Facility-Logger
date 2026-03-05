"""
================================================================================
MAS3 monitor
================================================================================
Description:    Scrapes real-time diagnostic data from a Bruker MAS unit's
                web interface and logs pneumatic pressures and spin rates
                to InfluxDB.

Functionality:  - Authenticates via HTTP sessions to the MAS controller EWP.
                - Parses HTML diagnostic tables using BeautifulSoup.
                - Extracts critical telemetry: Main/Tank pressures, Bearing
                  sensors, and Spin Rate (Hz).
                - Sanitizes string values into floats for time-series analysis.
                - Implements automatic re-authentication on session expiry
                  or connection drops.

Environment:    Requires MAS_IP_ADDRESS and InfluxDB credentials in .env.
Author:         Thomas Kress aided by ChatGPT
Date:           March 2026
================================================================================
"""

import os
import requests
from bs4 import BeautifulSoup
import time
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# Load configuration from environment variables
IP_ADDRESS = os.getenv("MAS_IP_ADDRESS")
USERNAME = os.getenv("MAS_USERNAME")
PASSWORD = os.getenv("MAS_PASSWORD")
INTERVAL_SECONDS = int(os.getenv("MAS_INTERVAL_SECONDS", 1))

INFLUXDB_URL = os.getenv("INFLUXDB_URL")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG")
INFLUXDB_BUCKET = os.getenv("MAS_INFLUXDB_BUCKET")
SPECTROMETER_NAME = os.getenv("SPECTROMETER_NAME")
ROOM_NAME = os.getenv("ROOM_NAME")
MANAGEMENT_NAME = os.getenv("MANAGEMENT_NAME")
OWNER_NAME = os.getenv("OWNER_NAME")

# Base URL and endpoints
BASE_URL = f"http://{IP_ADDRESS}/ewp"
LOGIN_URL = f"{BASE_URL}/login"
DATA_URL = f"{BASE_URL}/device/service/DiagnosisInformations/"

# Selected fields to log in InfluxDB
FIELDS_TO_LOG = [
    "Main Pressure (intern)",
    "Tank Pressure (extern)",
    "Bearing Sensor (mbar)",
    "Bearing Sense Sensor (mbar)",
    "Drive Sensor (mbar)",
    "Spin Rate (Hz)"
]

# Initialize session
session = requests.Session()
token = None

# Connect to InfluxDB v2
client = influxdb_client.InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = client.write_api(write_options=SYNCHRONOUS)


def logintomas():
    """Logs into the MAS system and retrieves a session token."""
    global token
    try:
        login_response = session.get(
            f"{LOGIN_URL}?user={USERNAME}&password={PASSWORD}&submittedForm=Login&submittedFormName=")

        if login_response.status_code == 200:
            soup = BeautifulSoup(login_response.text, "html.parser")
            token_input = soup.find("input", {"name": "session"})

            if token_input and "value" in token_input.attrs:
                token = token_input["value"]
                print("✅ Login successful, session token obtained.")
                return True
            else:
                print("❌ Session token not found.")
                return False
        else:
            print(f"❌ Login failed, status code: {login_response.status_code}")
            return False
    except requests.RequestException as e:
        print(f"❌ Error during login: {e}")
        return False


def parse_mas():
    """Fetches and parses the MAS diagnosis data."""
    global token
    if token is None:
        if not logintomas():
            return

    try:
        response = session.get(f"{DATA_URL}?session={token}")

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            extracted_data = {}

            for row in soup.find_all("tr"):
                field_name = row.find("div", class_="FormFieldName")
                field_value = row.find("div", class_="FormFieldValue")

                if field_name and field_value:
                    name = field_name.text.strip()
                    value = field_value.text.strip()
                    extracted_data[name] = value

            print("📊 Extracted Data:", extracted_data)

            # Log selected fields to InfluxDB
            log_to_influxdb(extracted_data)

        else:
            # Any response status other than 200 should trigger re-authentication
            print(f"❌ Failed to retrieve data, status code: {response.status_code}")
            print("⚠️ Re-authenticating...")
            if logintomas():
                # Retry parsing after re-login
                parse_mas()

    except requests.RequestException as e:
        print(f"❌ Error fetching data: {e}")
        print("⚠️ Connection error, attempting to re-authenticate...")
        if logintomas():
            # Retry parsing after re-login
            parse_mas()


def log_to_influxdb(data):
    """Logs selected fields to an existing InfluxDB bucket."""
    filtered_data = {}

    for key in FIELDS_TO_LOG:
        if key in data:
            value = data[key].split()[0]  # Remove units
            try:
                if value.replace('.', '', 1).isdigit() or value.lstrip('-').replace('.', '', 1).isdigit():
                    filtered_data[key] = float(value)
            except ValueError:
                print(f"⚠️ Warning: Could not parse value '{value}' for key '{key}'")

    if not filtered_data:
        print("⚠️ No valid data to log.")
        return

    # Create InfluxDB point with a tag for spectrometer name
    point = influxdb_client.Point("mas") \
        .tag("spectrometer", SPECTROMETER_NAME) \
        .tag("management", MANAGEMENT_NAME) \
        .tag("owner", OWNER_NAME) \
        .tag("room", ROOM_NAME)

    for key, value in filtered_data.items():
        print(key, value)
        point = point.field(key, value)

    try:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)
        print(
            f"✅ Logged to InfluxDB: Measurement='mas', Tag='spectrometer={SPECTROMETER_NAME}', Fields={filtered_data}")
    except Exception as e:
        print(f"❌ Error writing to InfluxDB: {e}")


# Run parse_mas every INTERVAL_SECONDS
if __name__ == "__main__":
    while True:
        try:
            parse_mas()
        except Exception as e:
            print(f"❌ Unexpected error in main loop: {e}")
            # Try to re-login if there's an unexpected error
            logintomas()

        time.sleep(INTERVAL_SECONDS)
