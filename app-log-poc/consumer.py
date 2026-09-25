from kafka import KafkaConsumer, KafkaProducer
import json
import os

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'app-logs')
KAFKA_DLQ_TOPIC = os.getenv('KAFKA_DLQ_TOPIC', 'app-logs-dlq')

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='log-consumer-group'
    )
    
    dlq_producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER]
    )

    print(f"Started reading from topic: {KAFKA_TOPIC}")
    try:
        for message in consumer:
            try:
                log_entry = json.loads(message.value.decode('utf-8'))
                
                # Check for structural poison pill (valid JSON, but wrong format)
                if not isinstance(log_entry, dict):
                    print(f"INVALID FORMAT DETECTED! Expected dictionary, got {type(log_entry).__name__}. Sending to DLQ ({KAFKA_DLQ_TOPIC})...")
                    dlq_producer.send(KAFKA_DLQ_TOPIC, message.value)
                    continue

                trace_id = log_entry.get('trace_id', 'UNKNOWN')
                msg = log_entry.get('message', '')
                print(f"Received log: trace_id={trace_id}, message='{msg}'")
                
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"POISON PILL DETECTED! Skipping invalid message and sending to DLQ ({KAFKA_DLQ_TOPIC}) - Error: {e}")
                dlq_producer.send(KAFKA_DLQ_TOPIC, message.value)
                continue
            except Exception as e:
                print(f"UNEXPECTED ERROR DETECTED! Sending to DLQ ({KAFKA_DLQ_TOPIC}) - Error: {e}")
                dlq_producer.send(KAFKA_DLQ_TOPIC, message.value if message.value else b'')
                continue
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        dlq_producer.flush()
        consumer.close()

if __name__ == "__main__":
    main()
