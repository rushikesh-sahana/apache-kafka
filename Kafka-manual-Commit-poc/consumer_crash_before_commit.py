from kafka import KafkaConsumer
import os
import time

TOPIC = "mannal-commit-topic"
GROUP_ID = "crash-demo"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",

    group_id=GROUP_ID,

    # Start from committed Kafka offset
    auto_offset_reset="earliest",

    # We manually control commits
    enable_auto_commit=False,

    value_deserializer=lambda v: int(v.decode("utf-8")),
)

print("===================================")
print("CRASH CONSUMER STARTED")
print("===================================")


for message in consumer:

    message_id = message.value

    print(
        f"[PROCESS] "
        f"message={message_id} "
        f"offset={message.offset}"
    )

    # Simulate business processing
    time.sleep(0.2)



    # --------------------------------
    # Process 100-149
    # but DON'T commit
    # --------------------------------

    if message_id < 150:
        continue

    # --------------------------------
    # When 150 arrives:
    # crash before committing
    # --------------------------------

    print()
    print("===================================")
    print("CRASH!")
    print("===================================")
    print("100-149 were processed")
    print("But their offsets were NOT committed")
    print("Application crashes before commit()")
    print()

    os._exit(1)
