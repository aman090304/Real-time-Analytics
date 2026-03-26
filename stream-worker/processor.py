import json
import time
from kafka import KafkaConsumer
import redis
from datetime import datetime

max_retries = 30
for attempt in range(max_retries):
    try:
        consumer = KafkaConsumer(
            "raw-events",
            bootstrap_servers="kafka:29092",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="analytics-group"
        )
        print("Connected to Kafka")
        break
    except Exception as e:
        if attempt < max_retries - 1:
            print(f"Attempt {attempt + 1}/{max_retries}: Waiting for Kafka... ({e})")
            time.sleep(2)
        else:
            print(f"Failed to connect to Kafka after {max_retries} attempts")
            raise

r = redis.Redis(host="redis", port=6379, decode_responses=True)

print("Stream processor started...")

for message in consumer:

    event = message.value
    event_id = event["event_id"]
    if r.setnx(f"processed:{event_id}", 1):
        r.expire(f"processed:{event_id}", 3600)

    # process event safely
    timestamp = datetime.fromtimestamp(event["timestamp"])
    minute_key = timestamp.strftime("%Y-%m-%d-%H-%M")
    route = f"{event['origin']}-{event['destination']}"
    route_key = f"{event['origin']}-{event['destination']}:{minute_key}"
    event_type = event["event_type"]
    device = event["device"]

    # increment route search count
    if event_type == "search":
        r.incr(f"search_count:{route}")

    # increment booking count
    if event_type == "booking":
        r.incr(f"booking_count:{route}")

    # device analytics
    r.incr(f"device:{device}")
    r.incr(f"search_count:{route_key}")
    # maintain sorted set of routes
    r.zincrby("top_routes", 1, route)
    key = f"search_count:{route_key}"
    r.incr(key)
    r.expire(key, 600)