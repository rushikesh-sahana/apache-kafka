from fastapi import FastAPI
from kafka import KafkaProducer
from pydantic import BaseModel

app = FastAPI(title="Kafka manual-commit scenario PoC")

TOPIC = "mannal-commit-topic"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: str(value).encode("utf-8"),
)


class ProduceRequest(BaseModel):
    start: int = 100
    end: int = 199


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
