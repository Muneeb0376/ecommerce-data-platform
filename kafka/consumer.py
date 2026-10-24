import json
import os
import time

import psycopg
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

TOPIC = "user_events"
BATCH_SIZE = 100
FLUSH_SECONDS = 2.0

DB = {
    "host": "localhost",
    "port": 5434,
    "dbname": "ecommerce",
    "user": "ecom_user",
    "password": os.getenv("POSTGRES_PASSWORD", ""),
}

INSERT_SQL = """
INSERT INTO stream_events (event_id, user_id, product_id, event_type, event_time)
VALUES (%(event_id)s, %(user_id)s, %(product_id)s, %(event_type)s, %(event_time)s)
ON CONFLICT (event_id) DO NOTHING
"""


def flush(conn, consumer, batch):
    if not batch:
        return
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, batch)
    conn.commit()
    consumer.commit(asynchronous=False)  # DB mein likhne ke BAAD commit
    print(f"Inserted {len(batch)} events")
    batch.clear()


def main():
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "stream-events-to-postgres",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    batch = []
    last_flush = time.time()
    print("Consuming. Ctrl+C to stop.")
    with psycopg.connect(**DB) as conn:
        try:
            while True:
                msg = consumer.poll(0.5)
                if msg is not None:
                    if msg.error():
                        print(f"Consumer error: {msg.error()}")
                    else:
                        batch.append(json.loads(msg.value()))
                if len(batch) >= BATCH_SIZE or (
                    batch and time.time() - last_flush >= FLUSH_SECONDS
                ):
                    flush(conn, consumer, batch)
                    last_flush = time.time()
        except KeyboardInterrupt:
            pass
        finally:
            flush(conn, consumer, batch)
            consumer.close()
            print("Stopped.")


if __name__ == "__main__":
    main()