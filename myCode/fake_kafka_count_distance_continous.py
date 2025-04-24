import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import math
import os

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'spark-consume'

# Constants for the simulation
ENTITY_ID = "duonghdt_test_distance_2"
TOTAL_DISTANCE_KM = 1000
DISTANCE_PER_MESSAGE = 1  # Distance to send for each message
MESSAGES_PER_RUN = 30  # Number of messages to send each time the code runs

# Load last position from file if it exists
def load_last_position():
    if os.path.exists('last_position.txt'):
        with open('last_position.txt', 'r') as f:
            return json.load(f)
    return {"distance_traveled": 0}  # Start from zero distance if file doesn't exist

def save_last_position(distance_traveled):
    with open('last_position.txt', 'w') as f:
        json.dump({"distance_traveled": distance_traveled}, f)

def create_distance_message(timestamp, latitude, longitude, distance):
    return {
        "entity_type": "VEHICLE",
        "entity_id": ENTITY_ID,
        "attribute_type": "SCOPE_CLIENT",
        "attribute_key": "datas",
        "logged": True,
        "bool_v": False,
        "str_v": "",
        "long_v": 0,
        "dbl_v": 0,
        "json_v": {
            "direction": random.randint(0, 359),
            "geocoding": "Fake address, Vietnam",
            "history": False,
            "latitude": latitude,
            "longitude": longitude,
            "odometer": 100000 + distance,  # Example odometer reading
            "speed": 1,  # Speed is constant as we send 1 km message per second
            "status": "run",
            "timestamp": timestamp
        },
        "last_update_ts": timestamp,
        "ts": timestamp,
        "value_type": "JSON",
        "value_nil": False,
        "new_attribute_key": "",
        "project_id": "3207b5c0-96f8-46f2-9c38-9a08a825ab6c",
        "not_send_ws": False,
        "AttributeSub": []
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
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Load the last position
    last_position = load_last_position()
    distance_traveled = last_position["distance_traveled"]

    # Calculate current latitude and longitude based on distance traveled
    current_latitude = 21.2 + (distance_traveled / 111)  # Roughly 111 km per degree latitude
    current_longitude = 105.5  # Assuming constant longitude for simplicity

    # Calculate how many messages to send this run
    messages_to_send = min(MESSAGES_PER_RUN, TOTAL_DISTANCE_KM - distance_traveled)

    # Initialize start time
    start_time = int((datetime(2025, 4, 24, 0, 0) - timedelta(days=20)).timestamp() * 1000)

    for i in range(messages_to_send):
        # Update distance traveled
        distance_traveled += DISTANCE_PER_MESSAGE

        # Create message with updated timestamp and position
        message = create_distance_message(start_time + (i * 1000), current_latitude, current_longitude, distance_traveled)
        producer.send(KAFKA_TOPIC, message)
        print(f"Message sent to Kafka topic: {KAFKA_TOPIC}, Entity ID: {ENTITY_ID}, Distance: {distance_traveled} km")

        # Save the last position
        save_last_position(distance_traveled)

        # Update positions for the next message
        current_latitude += DISTANCE_PER_MESSAGE / 111  # Update latitude for next message
        time.sleep(1)  # Wait for 1 second before sending the next message

    producer.flush()
    print("Finished sending distance messages.")

if __name__ == "__main__":
    main()