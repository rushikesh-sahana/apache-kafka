"""Small Kafka helper (kafka-python-ng): send one event, retry a few times."""

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders.events"

_producer = None


def _get_producer() -> KafkaProducer:
    """Create the producer lazily. If Kafka is down, this raises NoBrokersAvailable,
    which we catch in send_event and treat as a failed attempt."""
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            acks="all",  # wait for all in-sync replicas
            retries=0,  # WE control retries, not the library
            request_timeout_ms=3000,  # give up on a request after 3s
        )
    return _producer


# def _reset_producer() -> None:
#     """Drop the producer after a failure so a stale message can't be sent later
#     (which would cause a duplicate) and so the next attempt reconnects cleanly."""
#     global _producer
#     if _producer is not None:
#         try:
#             _producer.close(timeout=0)
#         except Exception:
#             pass
#         _producer = None


# def send_event(key: str, value: str, event_type: str) -> bool:
#     """One attempt. Returns True only if Kafka acknowledged the message."""
#     try:
#         producer = _get_producer()
#         future = producer.send(
#             TOPIC,
#             key=key.encode(),
#             value=value.encode(),
#             headers=[("event_type", event_type.encode())],
#         )
#         future.get(timeout=5)  # blocks until Kafka acks; raises KafkaError on failure
#         return True
#     except KafkaError as e:
#         print(f"  Kafka error: {type(e).__name__}")
#         _reset_producer()
#         return False
#
#
# def send_with_retry(key: str, value: str, event_type: str,
#                     retries: int = 3, delay: float = 1.0) -> bool:
#     for attempt in range(1, retries + 1):
#         if send_event(key, value, event_type):
#             print(f"  Kafka OK (attempt {attempt})")
#             return True
#         print(f"  Kafka attempt {attempt}/{retries} failed")
#         if attempt < retries:
#             time.sleep(delay)
#     return False

"""Small Kafka helper (kafka-python-ng): send one event with Kafka-managed retries."""

from kafka import KafkaProducer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders.events"

_producer = None


# def _get_producer() -> KafkaProducer:
#     """Create the producer lazily.
#
#     Kafka manages retries internally. Idempotence is enabled to reduce
#     duplicate records caused by producer retries.
#     """
#     global _producer
#
#     if _producer is None:
#         _producer = KafkaProducer(
#             bootstrap_servers=BOOTSTRAP_SERVERS,
#             acks="all",
#             retries=3,
#             request_timeout_ms=3000,
#             max_block_ms=3000,
#             api_version_auto_timeout_ms=3000,
#         )
#
#     return _producer


def send_event(key: str, value: str, event_type: str) -> bool:
    """Send one event.

    Returns True only when Kafka acknowledges the message.
    Kafka handles transient producer retries internally.
    """
    try:
        producer = _get_producer()

        future = producer.send(
            TOPIC,
            key=key.encode(),
            value=value.encode(),
            headers=[
                ("event_type", event_type.encode())
            ],
        )

        # Wait for Kafka to acknowledge the message.
        future.get(timeout=5)

        print("  Kafka OK")
        return True

    except KafkaError as e:
        print(f"  Kafka error: {type(e).__name__}")
        return False
