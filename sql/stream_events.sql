-- Stage 7: Kafka streaming. Consumer (kafka/consumer.py) yahan events likhta hai.
CREATE TABLE IF NOT EXISTS stream_events (
    event_id    UUID PRIMARY KEY,
    user_id     INT NOT NULL,
    product_id  INT NOT NULL,
    event_type  TEXT NOT NULL,
    event_time  TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);