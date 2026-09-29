"""Step 2: every 60 seconds, send ALL pending outbox events to Kafka, one by one (no batch).
Run only ONE instance (SQLite has no row-level locking)."""
import time

from db import get_conn, init_db
from kafka_client import send_event

INTERVAL_SECONDS = 60


def run_once() -> None:
    conn = get_conn()
    sent = 0
    try:
        while True:
            # Take the oldest pending event (one row at a time)
            row = conn.execute(
                "SELECT id, aggregate_id, event_type, payload FROM outbox "
                "WHERE processed_at IS NULL ORDER BY id LIMIT 1"
            ).fetchone()

            if row is None:
                break  # nothing left to send

            row_id, aggregate_id, event_type, payload = row
            if send_event(aggregate_id, payload, event_type):
                with conn:  # mark done immediately after Kafka confirms
                    conn.execute(
                        "UPDATE outbox SET processed_at = datetime('now') WHERE id = ?",
                        (row_id,),
                    )
                sent += 1
            else:
                print("Kafka still down, will retry next run")
                break  # stop; keeps order and avoids waiting on every row

        print(f"Sent {sent} pending event(s)" if sent else "Nothing sent this run")
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Scheduler started (every {INTERVAL_SECONDS}s)")
    while True:
        try:
            run_once()
        except Exception as e:
            print("Scheduler error:", e)
        time.sleep(INTERVAL_SECONDS)