"""
Kafka CONSUMER
--------------
A "consumer" is any app that SUBSCRIBES to (reads) messages from a Kafka TOPIC.
This one belongs to a "consumer group" — Kafka tracks, per group, how far
each partition has been read (the "offset"), so if you restart this script
it picks up where it left off instead of re-reading everything.
"""

import json

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "orders"

# All consumers sharing this group_id split the partitions between them —
# each message is delivered to only ONE consumer within the group.
GROUP_ID = "orders-consumer-group"


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        # Deserializer: converts bytes back into a Python dict.
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        # 'earliest' = if this is a brand-new group, start from the very
        # first message still retained on the topic (instead of only new ones).
        auto_offset_reset="earliest",
        # only after we've successfully processed the message.
        enable_auto_commit=True,
    )


def main():
    consumer = create_consumer()
    print(f"Consumer group '{GROUP_ID}' listening on topic '{TOPIC_NAME}'...\n")

    try:
        for record in consumer:
            print(
                f"Received: {record.value} "
                f"[partition={record.partition}, offset={record.offset}, key={record.key}]"
            )
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()