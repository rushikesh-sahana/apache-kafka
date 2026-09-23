"""
Kafka PRODUCER
--------------
A "producer" is any app that PUBLISHES (writes) messages to a Kafka TOPIC.
Here we send a fixed, static list of "order event" messages (one per
second) to the topic "orders" — so every run publishes the exact same data,
which is handy while you're still learning and want predictable output.
"""

import json
import time

from kafka import KafkaProducer

# Where the Kafka broker lives (set up via docker-compose.yml)
BOOTSTRAP_SERVERS = "localhost:9092"

# The "topic" is like a named channel/category messages get published to.
TOPIC_NAME = "orders"

# --- Static sample data -----------------------------------------------
# Fixed, hand-written records instead of randomly generated ones, so the
# output is identical and predictable on every run.
STATIC_ORDERS = [
    {"order_id": 1, "item": "widget",     "quantity": 2, "customer": "alice"},
    {"order_id": 2, "item": "gadget",     "quantity": 1, "customer": "bob"},
    {"order_id": 3, "item": "gizmo",      "quantity": 5, "customer": "carol"},
    {"order_id": 4, "item": "widget",     "quantity": 3, "customer": "dave"},
    {"order_id": 5, "item": "thingamajig","quantity": 1, "customer": "erin"},
]


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer connected to our broker."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Serializer: converts our Python dict -> bytes before sending on the wire.
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # key_serializer determines which PARTITION a message lands in.
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def main():
    producer = create_producer()
    print(f"Producer connected. Publishing {len(STATIC_ORDERS)} static records to topic '{TOPIC_NAME}'...\n")

    try:
        for message in STATIC_ORDERS:
            # KEY: used by Kafka to decide the partition (same key -> same partition).
            # Using order_id as key here just for demonstration.
            key = str(message["order_id"])

            # send() is async — it returns a "future". We block with get() just
            # so we can print confirmation of exactly which partition/offset we landed on.
            future = producer.send(TOPIC_NAME, key=key, value=message)
            record_metadata = future.get(timeout=10)

            print(
                f"Sent: {message} "
                f"-> partition={record_metadata.partition}, offset={record_metadata.offset}"
            )

            time.sleep(1)

        print("\nAll static records published. Exiting.")

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # flush() makes sure any buffered messages are actually sent before we exit.
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()