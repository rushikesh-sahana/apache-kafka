import json
import subprocess

from fastapi import FastAPI
from kafka import KafkaProducer
from pydantic import BaseModel

app = FastAPI(title="Kafka manual-commit scenario PoC")

TOPIC = "mannal-commit-topic"
BOOTSTRAP_SERVERS = "localhost:9092"
consumer_process = None

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    # Serializer: converts our Python dict -> bytes before sending on the wire.
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    # key_serializer determines which PARTITION a message lands in.
    key_serializer=lambda k: k.encode("utf-8") if k else None,
    acks='all',  # wait for all replicas to ack
    retries=3,
    request_timeout_ms=30_000,
)


class ProduceRequest(BaseModel):
    start: int = 100
    end: int = 200


@app.get("/")
def root():
    return {
        "application": "Kafka Producer",
        "status": "running",
    }


@app.post("/produce")
def produce_messages(request: ProduceRequest):

    results = []

    for message_id in range(request.start, request.end + 1):

        future = producer.send(
            TOPIC,
            value=message_id,
        )

        metadata = future.get(timeout=10)

        results.append({
            "message_id": message_id,
            "partition": metadata.partition,
            "offset": metadata.offset,
        })

    producer.flush()

    return {
        "topic": TOPIC,
        "count": len(results),
        "messages": results,
    }

@app.post("/consumer_crash_befor_commit/start")
def start_consumer():

    global consumer_process

    if consumer_process is not None:
        if consumer_process.poll() is None:
            return {
                "status": "already running",
                "pid": consumer_process.pid
            }

    consumer_process = subprocess.Popen(
        ["python3", "consumer_crash_before_commit.py"]
    )

    return {
        "status": "started",
        "pid": consumer_process.pid
    }


@app.post("/consumer_crash_befor_commit/stop")
def stop_consumer_crash():

    global consumer_process

    if consumer_process is None:
        return {
            "status": "not running"
        }

    if consumer_process.poll() is not None:
        return {
            "status": "already stopped"
        }

    consumer_process.terminate()

    consumer_process.wait()

    return {
        "status": "stopped"
    }



@app.get("/consumer_crash_befor_commit/status")
def consumer_status():

    if consumer_process is None:
        return {
            "running": False
        }

    running = consumer_process.poll() is None

    return {
        "running": running,
        "pid": consumer_process.pid
    }

@app.post("/consumer_idempotent/start")
def start_consumer():

    global consumer_process

    if consumer_process is not None:
        if consumer_process.poll() is None:
            return {
                "status": "already running",
                "pid": consumer_process.pid
            }

    consumer_process = subprocess.Popen(
        ["python3", "consumer_idempotent.py"]
    )

    return {
        "status": "started",
        "pid": consumer_process.pid
    }

@app.post("/consumer_idempotent/stop")
def stop_consumer_crash():

    global consumer_process

    if consumer_process is None:
        return {
            "status": "not running"
        }

    if consumer_process.poll() is not None:
        return {
            "status": "already stopped"
        }

    consumer_process.terminate()

    consumer_process.wait()

    return {
        "status": "stopped"
    }