import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "user_events"
BOOTSTRAP = "localhost:9092"

MAX_USER_ID = 100_000
MAX_PRODUCT_ID = 1_000

EVENT_WEIGHTS = {
    "product_view": 70,
    "add_to_cart": 20,
    "purchase": 10,
}


def make_event() -> dict:
    event_type = random.choices(
        list(EVENT_WEIGHTS), weights=list(EVENT_WEIGHTS.values())
    )[0]
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, MAX_USER_ID),
        "product_id": random.randint(1, MAX_PRODUCT_ID),
        "event_type": event_type,
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def on_delivery(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    print(f"Sending events to '{TOPIC}'. Ctrl+C to stop.")
    sent = 0
    try:
        while True:
            event = make_event()
            producer.produce(
                TOPIC,
                key=str(event["user_id"]),
                value=json.dumps(event),
                callback=on_delivery,
            )
            producer.poll(0)
            sent += 1
            if sent % 20 == 0:
                print(f"{sent} events sent, last: {event['event_type']}")
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"Done. {sent} events sent.")


if __name__ == "__main__":
    main()