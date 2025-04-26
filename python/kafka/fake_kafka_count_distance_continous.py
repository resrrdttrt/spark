import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import math
import os

# Kafka configuration
KAFKA_BROKER = '172.21.6.68:9092'
KAFKA_TOPIC = 'sparkts'

# Constants for the simulation
ENTITY_ID = "duonghdt_test_distance_7"
PLATE_NO = "VN300475"
TOTAL_DISTANCE_KM = 10000
DISTANCE_PER_MESSAGE = 1  # Distance to send for each message
MESSAGES_PER_RUN = 30  # Number of messages to send each time the code runs

# Placeholder driver information (replace with actual values if available)
DRIVER_NAME = "John Doe"
DRIVER_LICENSE = "DL123456"

# Load last position and start time from file if it exists
def load_last_position():
    if os.path.exists('last_position.txt'):
        with open('last_position.txt', 'r') as f:
            return json.load(f)
    # Initialize with default values if file doesn't exist
    default_start_time = int((datetime(2025, 4, 24, 0, 0) + timedelta(days=1)).timestamp() * 1000)
    return {"distance_traveled": 0, "start_time": default_start_time}

def save_last_position(distance_traveled, start_time):
    with open('last_position.txt', 'w') as f:
        json.dump({"distance_traveled": distance_traveled, "start_time": start_time}, f)

def create_distance_message(timestamp, latitude, longitude):
    return {
        "id": ENTITY_ID,
        "status": "run",
        "speed": 1,  # Constant speed as per original logic
        "direction": random.randint(0, 359),
        "geocoding": "Fake address, Vietnam",
        "latitude": latitude,
        "longitude": longitude,
        "ts": timestamp,
        "plate_no": PLATE_NO,
        "driver_name": DRIVER_NAME,
        "driver_license": DRIVER_LICENSE,
        "history": False,
        "extrainfo": {
            "aircon": random.choice([True, False]),
            "door": random.choice([True, False]),
            "driverLicense": DRIVER_LICENSE,
            "driverName": DRIVER_NAME,
            "ignition": random.choice([True, False]),
            "ts": timestamp
        }
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

def main():
    # Initialize Kafka producer with error handling
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,  # Retry sending messages on failure
            acks='all'  # Wait for acknowledgment from all replicas
        )
        print(f"[INFO] Connected to Kafka broker: {KAFKA_BROKER}")
    except Exception as e:
        print(f"[ERROR] Failed to connect to Kafka broker {KAFKA_BROKER}: {str(e)}")
        return

    # Load the last position and start time
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
        current_time = start_time + (i + 1) * 1000 + 60*1000*0*0 # Increment by 1 second per message

        # Create message with updated timestamp and position
        message = create_distance_message(current_time, current_latitude, current_longitude)
        try:
            producer.send(KAFKA_TOPIC, message)
            print(f"[INFO] Message sent to Kafka topic: {KAFKA_TOPIC}, ID: {ENTITY_ID}, Distance: {distance_traveled} km, TS: {current_time}")
        except Exception as e:
            print(f"[ERROR] Failed to send message to Kafka topic {KAFKA_TOPIC}: {str(e)}")

        # Save the last position and timestamp
        save_last_position(distance_traveled, current_time)

        # Update positions for the next message
        current_latitude += DISTANCE_PER_MESSAGE / 111  # Update latitude for next message
        # time.sleep(1)  # Wait for 1 second before sending the next message

    # Flush and close producer
    try:
        producer.flush()
        producer.close()
        print("[INFO] Finished sending distance messages.")
    except Exception as e:
        print(f"[ERROR] Failed to flush/close Kafka producer: {str(e)}")

if __name__ == "__main__":
    # Note: For downstream Spark processing, set up the environment as follows:
    # export SPARK_HOME=/opt/spark
    # export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    # export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
    main()