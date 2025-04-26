import json
import time
import random
import requests
from datetime import datetime, timedelta
import os
import math

# HTTP configuration
URL = 'http://172.21.6.63:32735/api/attributes/vtracking/VEHICLE/8fb92b58-da6f-4da7-980f-549310e04a1b/values?logged=true'
HEADERS = {
    'Authorization': 'Bearer wXoBrtOwutqIZzJ4lynChUSpFo1aBtSa',
    'Content-Type': 'application/json'
}

# Constants for the simulation
ENTITY_ID = "8fb92b58-da6f-4da7-980f-549310e04a1b"
PLATE_NO = "VN300475"
TOTAL_DISTANCE_KM = 10000
DISTANCE_PER_MESSAGE = 1  # Distance to send for each message
MESSAGES_PER_RUN = 30  # Number of messages to send each time the code runs
RETRY_ATTEMPTS = 3  # Number of retries for failed HTTP requests

# Placeholder driver information (replace with actual values if available)
DRIVER_NAME = "John Doe"
DRIVER_LICENSE = "DL123456"

# Load last position and start time from file if it exists
def load_last_position():
    if os.path.exists('last_position.txt'):
        with open('last_position.txt', 'r') as f:
            return json.load(f)
    # Initialize with default values if file doesn't exist
    default_start_time = int((datetime(2025, 4, 1, 0, 0) + timedelta(days=1)).timestamp() * 1000)
    return {"distance_traveled": 0, "start_time": default_start_time}

def save_last_position(distance_traveled, start_time):
    with open('last_position.txt', 'w') as f:
        json.dump({"distance_traveled": distance_traveled, "start_time": start_time}, f)

def create_distance_message(timestamp, latitude, longitude):
    return {
        "datas": {
            "id": ENTITY_ID,
            "status": "run",
            "speed": 1,  # Constant speed
            "direction": random.randint(0, 359),
            "geocoding": "Yên Nghĩa, Phường Yên Nghĩa, Quận Hà Đông, Thành phố Hà Nội, Việt Nam",
            "latitude": latitude,
            "longitude": longitude,
            "plate_no": PLATE_NO,
            "driver_name": DRIVER_NAME,
            "driver_license": DRIVER_LICENSE,
            "history": False,
            "aircon": random.choice([True, False]),
            "door": random.choice([True, False]),
            "ignition": random.choice([True, False]),
            "extrainfo": {
                "aircon": random.choice([True, False]),
                "door": random.choice([True, False]),
                "driverLicense": DRIVER_LICENSE,
                "driverName": DRIVER_NAME,
                "ignition": random.choice([True, False]),
                "ts": timestamp
            }
        },
        "ts": timestamp
    }

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    return R * c  # Distance in kilometers

def send_data(message, distance_traveled):
    print("Start send data")
    for attempt in range(RETRY_ATTEMPTS):
        try:
            print("Start Post")
            response = requests.post(URL, headers=HEADERS, json=message)
            if response.status_code == 200:
                print(f"[{datetime.now()}] Sent data: ID={message['datas']['id']}, Distance={distance_traveled} km, TS={message['ts']} → Status: {response.status_code}")
                return True
            else:
                print(f"[{datetime.now()}] Failed to send data: Status: {response.status_code}, Response: {response.text}")
        except Exception as e:
            print(f"[{datetime.now()}] Error sending data (attempt {attempt + 1}/{RETRY_ATTEMPTS}): {str(e)}")
        print("Start sleep")
        time.sleep(1)  # Wait before retrying
    return False

def main():
    # Load the last position and start time
    print("START")
    last_position = load_last_position()
    distance_traveled = last_position["distance_traveled"]
    start_time = last_position["start_time"]

    # Calculate current latitude and longitude based on distance traveled
    current_latitude = 21.2 + (distance_traveled / 111)  # Roughly 111 km per degree latitude
    current_longitude = 105.5  # Assuming constant longitude for simplicity

    # Calculate how many messages to send this run
    messages_to_send = min(MESSAGES_PER_RUN, TOTAL_DISTANCE_KM - distance_traveled)

    # Initialize timestamp for this run, continuing from last checkpoint
    current_time = start_time

    for i in range(messages_to_send):
        # Update distance traveled
        distance_traveled += DISTANCE_PER_MESSAGE
        # Update timestamp for this message
        current_time = start_time + (i + 1) * 1000 + 60*1000*0  # Increment by 1 second per message

        # Create message with updated timestamp and position
        message = create_distance_message(current_time, current_latitude, current_longitude)
        
        # Send data via HTTP
        if not send_data(message, distance_traveled):
            print(f"[{datetime.now()}] Failed to send message after {RETRY_ATTEMPTS} attempts. Stopping.")
            break

        # Save the last position and timestamp
        save_last_position(distance_traveled, current_time)

        # Update positions for the next message
        current_latitude += DISTANCE_PER_MESSAGE / 111  # Update latitude for next message
        # time.sleep(1)  # Wait for 1 second before sending the next message

    print("[INFO] Finished sending distance messages.")

if __name__ == "__main__":
    # Note: For downstream Spark processing, set up the environment as follows:
    # export SPARK_HOME=/opt/spark
    # export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    # export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
    main()