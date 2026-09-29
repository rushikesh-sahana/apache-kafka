"""Step 1: save the order, try Kafka directly (3 retries).
If Kafka is still down -> store the event in the outbox table for the scheduler."""
import json
import sys
import uuid

from db import get_conn, init_db
from kafka_client import send_event


def create_order(customer: str, amount: float) -> str:
    order_id = str(uuid.uuid4())
    event = {
        "event_id": str(uuid.uuid4()),  # same id whether sent now or later -> consumer can de-dup
        "order_id": order_id,
        "customer": customer,
        "amount": amount,
    }
    payload = json.dumps(event)

    conn = get_conn()
    try:
        # 1) Save the business data
        with conn:
            conn.execute(
                "INSERT INTO orders (id, customer, amount) VALUES (?, ?, ?)",
                (order_id, customer, amount),
            )

        # 2) Try Kafka directly, 3 attempts
        if send_event(order_id, payload, "OrderCreated"):
            return order_id

        # 3) Kafka still down -> park the event in the outbox
        with conn:
            conn.execute(
                "INSERT INTO outbox (aggregate_id, event_type, payload) VALUES (?, ?, ?)",
                (order_id, "OrderCreated", payload),
            )
        print("  Kafka down -> event saved to outbox, scheduler will retry")
    finally:
        conn.close()
    return order_id


if __name__ == "__main__":
    init_db()
    customer = sys.argv[1] if len(sys.argv) > 1 else "ts"
    amount = float(sys.argv[2]) if len(sys.argv) > 2 else 99.5
    print("Created order:", create_order(customer, amount))