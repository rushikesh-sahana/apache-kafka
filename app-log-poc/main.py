from fastapi import FastAPI
from kafka import KafkaProducer
import json
import uuid
import time
import os

app = FastAPI()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "app-logs")


# --------------------------------------------------
# Custom serializer
# --------------------------------------------------
# Normal Python objects:
#     dict/list -> JSON -> bytes
#
# bytes:
#     send directly to Kafka without modification
#
# This allows us to send a deliberately malformed
# JSON message (poison pill) using the SAME producer.
# --------------------------------------------------
def serialize_value(value):
    if isinstance(value, bytes):
        return value

    return json.dumps(value).encode("utf-8")


# --------------------------------------------------
# ONE Kafka producer
# --------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],

    # Python dict/list -> JSON bytes
    value_serializer=serialize_value,

    # trace_id -> Kafka message key
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)


# --------------------------------------------------
# Generate normal logs
# --------------------------------------------------
@app.post("/generate-logs")
async def generate_logs():

    trace_id = str(uuid.uuid4())

    for i in range(10):

        log_message = {
            "trace_id": trace_id,
            "log_level": "INFO",
            "message": f"Log message {i + 1} for trace {trace_id}",
            "timestamp": time.time()
        }

        producer.send(
            KAFKA_TOPIC,
            key=trace_id,
            value=log_message
        )

    producer.flush()

    return {
        "message": "10 logs generated successfully",
        "trace_id": trace_id
    }


# --------------------------------------------------
# Generate poison pill scenario
# --------------------------------------------------
@app.post("/generate-poison-pill")
async def generate_poison_pill():

    trace_id = str(uuid.uuid4())

    # --------------------------------------------------
    # Message 1-5: Valid JSON dictionaries
    # --------------------------------------------------
    for i in range(5):

        log_message = {
            "trace_id": trace_id,
            "log_level": "INFO",
            "message": f"Valid log {i + 1} before poison pill",
            "timestamp": time.time()
        }

        producer.send(
            KAFKA_TOPIC,
            key=trace_id,
            value=log_message
        )

    # --------------------------------------------------
    # Message 6: POISON PILL
    # Malformed JSON
    # --------------------------------------------------
    poison_pill_msg = b"{this is a malformed json: missing quotes}"

    producer.send(
        KAFKA_TOPIC,
        key=trace_id,
        value=poison_pill_msg
    )

    # --------------------------------------------------
    # Message 7: POISON PILL
    # Valid JSON, but wrong format
    # --------------------------------------------------
    wrong_format_msg = [
        "this",
        "is",
        "a",
        "list",
        "not",
        "a",
        "dict"
    ]

    producer.send(
        KAFKA_TOPIC,
        key=trace_id,
        value=wrong_format_msg
    )

    # --------------------------------------------------
    # Message 8-10: Valid JSON dictionaries
    # --------------------------------------------------
    for i in range(8, 11):

        log_message = {
            "trace_id": trace_id,
            "log_level": "INFO",
            "message": f"Valid log {i} after poison pills",
            "timestamp": time.time()
        }

        producer.send(
            KAFKA_TOPIC,
            key=trace_id,
            value=log_message
        )

    # Make sure all 10 messages are sent
    producer.flush()

    return {
        "message": "10 messages generated successfully",
        "trace_id": trace_id,
        "messages": {
            "1-5": "Valid JSON",
            "6": "Malformed JSON - Poison Pill",
            "7": "Wrong JSON format - Poison Pill",
            "8-10": "Valid JSON"
        }
    }