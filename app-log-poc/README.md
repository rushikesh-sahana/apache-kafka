# Kafka Poison Pill & Concurrency POC

This Proof of Concept (POC) demonstrates a robust data pipeline using **FastAPI** as a producer and a **Python Kafka Consumer**, designed to handle high concurrency and resilience against "Poison Pill" messages.

## What is a Poison Pill in Kafka?
In Kafka, a **Poison Pill** is a message that is fundamentally malformed or incompatible with the consumer's expected data format (e.g., a broken JSON string, or a list instead of a dictionary). If not handled correctly, a poison pill will cause the consumer to crash or get stuck in an infinite loop trying to process the same broken message, halting the entire pipeline.

### How this POC solves it:
1. **Robust Deserialization**: The consumer does not use Kafka's built-in `value_deserializer` which crashes on failure. Instead, it reads raw bytes and manually decodes them inside a `try/except` block.
2. **Dead Letter Queue (DLQ)**: If a poison pill is detected (either structural or malformed JSON), the consumer logs an error, routes the bad message to a separate Kafka topic (`app-logs-dlq`), and smoothly continues processing the rest of the queue.

## Features Included
1. **Standard Logging**: Generate 10 standard valid logs at once.
2. **Poison Pill Simulation**: Generates a sequence of exactly 10 messages:
   - 5 valid JSON logs
   - 1 Poison Pill: Malformed JSON string (`b"{this is a malformed json: missing quotes}"`)
   - 1 Poison Pill: Structurally wrong JSON (`["list", "instead", "of", "dict"]`)
   - 3 valid JSON logs
3. **High Concurrency Testing**: A script to simulate 50 concurrent requests pushing hundreds of logs to Kafka simultaneously.

## Prerequisites
- A running Kafka & Zookeeper/KRaft broker (Default: `localhost:9092`)
- Python 3.8+

## Setup Instructions

1. **Activate Environment**
```bash
source app-log-poc-venv/bin/activate
```

2. **Install Requirements**
```bash
pip install -r requirements.txt
```

## Running the Application

### 1. Start the Producer (FastAPI)
Run the following command in Terminal 1:
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 2. Start the Consumer
Run the following command in Terminal 2:
```bash
python consumer.py
```

### 3. Test the Endpoints

You can trigger the APIs via the interactive Swagger UI at: http://localhost:8000/docs

Alternatively, use `curl`:

**Trigger Standard Logs:**
```bash
curl -X POST http://localhost:8000/generate-logs
```

**Trigger Poison Pill Scenario:**
```bash
curl -X POST http://localhost:8000/generate-poison-pill
```
*Watch your consumer terminal to see logs 1-5 process successfully, logs 6 and 7 trigger warnings and get routed to the DLQ, and logs 8-10 process successfully!*

### 4. Run the Load Test
To see how the system handles a sudden influx of messages, run the concurrency script in Terminal 3:
```bash
python concurrent_requests.py
```
This will hit the API 50 times simultaneously, pushing 500 logs to the Kafka topic.
