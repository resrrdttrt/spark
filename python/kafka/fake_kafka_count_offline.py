import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Kafka configuration
KAFKA_BROKER = '172.21.6.68:9092'
KAFKA_TOPIC = 'sparkts'

# Generate 10 fake entity IDs
ENTITY_IDS = [f"3fa541ad-dda7-4bd8-bdbd-8f78d2f7600{i}" for i in range(10)]

# Generate a list of timestamps (one per day over last 10 days)
TIMESTAMPS = [
    int((datetime.now() - timedelta(days=i)).timestamp() * 1000)
    for i in range(10)
]

offline_count = 0

def create_message(index):
    # Use the original entity ID for the message
    global offline_count
    entity_id = ENTITY_IDS[index % len(ENTITY_IDS)]
    ts = TIMESTAMPS[index % len(TIMESTAMPS)]
    latitude = round(21.2 + random.uniform(-0.05, 0.05), 6)
    longitude = round(105.5 + random.uniform(-0.05, 0.05), 6)
    speed = random.randint(0, 100)

    # Set status to "offline" if the message is from the current day
    current_day_start = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    status = "offline" if ts >= current_day_start else ("run" if speed > 0 else "stop")

    # Replace entity_id with a unique value if status is "offline"
    if status == "offline":
        entity_id = str(uuid.uuid4())  # Generate a unique UUID
        print(f"Message with offline status at {datetime.fromtimestamp(ts / 1000)} and unique id: {entity_id}")
        offline_count = offline_count + 1

    # Generate fake extrainfo fields
    driver_name = f"Driver_{random.randint(100, 999)}"
    driver_license = f"DL{random.randint(100000, 999999)}"

    return {
        "id": entity_id,
        "status": status,
        "speed": speed,
        "direction": random.randint(0, 359),
        "geocoding": "Fake address, Vietnam",
        "latitude": latitude,
        "longitude": longitude,
        "ts": ts,
        "plate_no": f"VN{random.randint(1000, 9999)}",
        "driver_name": driver_name,
        "driver_license": driver_license,
        "history": False,
        "extrainfo": {
            "aircon": random.choice([True, False]),
            "door": random.choice([True, False]),
            "driverLicense": driver_license,
            "driverName": driver_name,
            "ignition": random.choice([True, False]),
            "ts": ts
        }
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    start_time = time.time()
    end_time = start_time + 30  # Run for 30 seconds
    message_count = 0

    while time.time() < end_time:
        for _ in range(3):  # Send 3 messages per second
            message = create_message(message_count)
            producer.send(KAFKA_TOPIC, message)
            message_count += 1
            print(f"Message {message_count} sent to Kafka topic: {KAFKA_TOPIC}")

        time.sleep(1)

    producer.flush()
    print(f"Finished sending {message_count} messages.")
    print(f"Finished sending {offline_count} different offline messages.")

if __name__ == "__main__":
    main()