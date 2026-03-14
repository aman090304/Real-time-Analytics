import json
import uuid
import time
import random
from kafka import KafkaProducer
from config import EVENT_TYPES, random_airport, DEVICES
import os

GENERATOR_ID = os.getenv("GENERATOR_ID", "generator-1")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

max_retries = 30
for attempt in range(max_retries):
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=5,
            batch_size=32768,
            acks=0,
            api_version=(0, 10),
            metadata_max_age_ms=30000,
            request_timeout_ms=60000
        )
        print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        break
    except Exception as e:
        if attempt < max_retries - 1:
            print(f"Attempt {attempt + 1}/{max_retries}: Waiting for Kafka... ({e})")
            time.sleep(2)
        else:
            print(f"Failed to connect to Kafka after {max_retries} attempts")
            raise

def generate_event():
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "user_id": random.randint(1, 1000000),
        "origin": random_airport(),
        "destination": random_airport(),
        "device": random.choice(DEVICES),
        "timestamp": int(time.time())
    }

BASE_EVENT_RATE = 5000

while True:
    try:
        start = time.time()
        if random.random() < 0.1:
            EVENT_RATE = random.randint(10000, 20000)
        else:
            EVENT_RATE = BASE_EVENT_RATE

        for _ in range(EVENT_RATE):
            producer.send("raw-events", generate_event())

        producer.flush()

        elapsed = time.time() - start

        print(f"Sent {EVENT_RATE} events in {elapsed:.2f}s")

        if elapsed < 1:
            time.sleep(1 - elapsed)
    except Exception as e:
        print(f"Error during event generation/sending: {e}")
        time.sleep(5)
        continue