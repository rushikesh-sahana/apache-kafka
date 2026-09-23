"""
Kafka PRODUCER
--------------
A "producer" is any app that PUBLISHES (writes) messages to a Kafka TOPIC.
Here we send a fixed, static list of "order event" messages (one per
second) to the topic "orders" — so every run publishes the exact same data,
which is handy while you're still learning and want predictable output.
"""

import json
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

# Where the Kafka broker lives (set up via docker-compose.yml)
BOOTSTRAP_SERVERS = "localhost:9092"

# The "topic" is like a named channel/category messages get published to.
TOPIC_NAME = "orders"

# --- Static sample data -----------------------------------------------
# Fixed, hand-written records instead of randomly generated ones, so the
# output is identical and predictable on every run.
STATIC_ORDERS = [
    {"order_id": 1, "item": "widget",      "quantity": 2, "customer": "alice"},
    {"order_id": 2, "item": "gadget",      "quantity": 1, "customer": "bob"},
    {"order_id": 3, "item": "gizmo",       "quantity": 5, "customer": "carol"},
    {"order_id": 4, "item": "widget",      "quantity": 3, "customer": "dave"},
    {"order_id": 5, "item": "thingamajig", "quantity": 1, "customer": "erin"},
]


def create_producer() -> KafkaProducer:
    """Create and return a KafkaProducer connected to our broker."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        # Serializer: converts our Python dict -> bytes before sending on the wire.
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # key_serializer determines which PARTITION a message lands in.
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks='all',  # wait for all replicas to ack
        retries=3,
        request_timeout_ms=30_000,
    )


def main():
    # 1) Connecting to the broker can fail (broker down, wrong host/port, etc.)
    try:
        producer = create_producer()
    except NoBrokersAvailable:
        print(f"ERROR: Could not reach any Kafka broker at '{BOOTSTRAP_SERVERS}'. "
              f"Is it running?")
        sys.exit(1)
    except KafkaError as e:
        print(f"ERROR: Failed to create producer: {e}")
        sys.exit(1)

    print(f"Producer connected. Publishing {len(STATIC_ORDERS)} static records to topic '{TOPIC_NAME}'...\n")

    try:
        for message in STATIC_ORDERS:
            # KEY: used by Kafka to decide the partition (same key -> same partition).
            # Using order_id as key here just for demonstration.
            key = str(message["order_id"])

            try:
                # send() is async — it returns a "future". We block with get() just
                # so we can print confirmation of exactly which partition/offset we landed on.
                future = producer.send(TOPIC_NAME, key=key, value=message)
                record_metadata = future.get(timeout=10)

                print(
                    f"Sent: {message} "
                    f"-> partition={record_metadata.partition}, offset={record_metadata.offset}"
                )
            except KafkaTimeoutError:
                print(f"ERROR: Timed out sending {message} (no ack within 10s). Skipping.")
            except KafkaError as e:
                print(f"ERROR: Failed to send {message}: {e}. Skipping.")

            time.sleep(1)

    except KeyboardInterrupt:
        print("\nInterrupted by user. Shutting down...")

    finally:
        # Make sure everything buffered actually gets delivered before we exit,
        # even if we hit an error or Ctrl+C above.
        try:
            producer.flush(timeout=10)
        except KafkaError as e:
            print(f"WARNING: Error flushing producer: {e}")
        producer.close()

    print("\nAll static records published. Exiting.")


if __name__ == "__main__":
    main()