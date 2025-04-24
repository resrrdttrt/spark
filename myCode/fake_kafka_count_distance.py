import json
import time
import random
from datetime import datetime, timedelta
from kafka import KafkaProducer
import math

# Kafka configuration
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'spark-consume'

# Constants for the simulation
ENTITY_ID = "duonghdt_test_distance_2"
TOTAL_DISTANCE_KM = 400
DURATION_SECONDS = 30
SPEED_KMH = TOTAL_DISTANCE_KM / (DURATION_SECONDS / 3600)  # Speed in km/h

# Starting position (latitude and longitude)
START_LATITUDE = 21.2
START_LONGITUDE = 105.5

def create_distance_message(timestamp, latitude, longitude):
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
            "odometer": 100000,  # Example odometer reading
            "speed": SPEED_KMH,  # Set speed
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

    # Initialize start time and calculate timestamps
    start_time = int((datetime(2025, 4, 24, 0, 0) - timedelta(days=3)).timestamp() * 1000)
    current_latitude = START_LATITUDE
    current_longitude = START_LONGITUDE

    # Calculate distance covered per message (in km)
    distance_per_message = TOTAL_DISTANCE_KM / (DURATION_SECONDS * 3)  # 3 messages per second
    total_messages = DURATION_SECONDS * 3  # Total messages to send

    previous_latitude = current_latitude
    previous_longitude = current_longitude
    total_traveled_distance = 0.0

    for i in range(total_messages):
        # Calculate new position based on distance
        current_latitude += distance_per_message / 111  # Roughly 111 km per degree latitude
        current_longitude += distance_per_message / (111 * abs(current_latitude))  # Adjust for longitude

        # Create message with updated timestamp and position
        message = create_distance_message(start_time + (i * 333), current_latitude, current_longitude)
        producer.send(KAFKA_TOPIC, message)
        print(f"Message sent to Kafka topic: {KAFKA_TOPIC}, Entity ID: {ENTITY_ID}")

        # Calculate distance traveled using Haversine formula
        distance = haversine(previous_latitude, previous_longitude, current_latitude, current_longitude)
        total_traveled_distance += distance

        # Update previous position
        previous_latitude = current_latitude
        previous_longitude = current_longitude

        time.sleep(1 / 3)  # Wait for 1/3 of a second to simulate 3 messages per second

    producer.flush()
    print(f"Total distance traveled: {total_traveled_distance:.2f} km")
    print("Finished sending distance messages.")

if __name__ == "__main__":
    main()
