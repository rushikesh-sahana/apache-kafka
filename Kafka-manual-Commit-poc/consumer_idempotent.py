from kafka import KafkaConsumer
import sqlite3
import time

TOPIC = "mannal-commit-topic"
GROUP_ID = "idempotent-demo"

DB_FILE = "business.db"


# ==========================================
# Database
# ==========================================

db = sqlite3.connect(DB_FILE)

db.execute("""
CREATE TABLE IF NOT EXISTS processed_messages (
    message_id INTEGER PRIMARY KEY,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
)
""")

db.commit()


# ==========================================
# Kafka
# ==========================================

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",

    group_id=GROUP_ID,

    auto_offset_reset="earliest",

    enable_auto_commit=False,

    value_deserializer=lambda v: int(v.decode("utf-8")),
)


# ==========================================
# Business processing
# ==========================================

def process_message(message_id):

    # Check idempotency
    row = db.execute(
        """
        SELECT message_id
        FROM processed_messages
        WHERE message_id = ?
        """,
        (message_id,)
    ).fetchone()

    if row:

        print(
            f"[DUPLICATE] message={message_id} "
            f"already processed -> SKIP"
        )

        return

    # --------------------------------------
    # Actual business operation
    # --------------------------------------

    print(
        f"[BUSINESS] processing message={message_id}"
    )

    time.sleep(0.3)

    # --------------------------------------
    # Record successful processing
    # --------------------------------------

    db.execute(
        """
        INSERT INTO processed_messages(message_id)
        VALUES (?)
        """,
        (message_id,)
    )

    db.commit()

    print(
        f"[DB] message={message_id} recorded"
    )


# ==========================================
# Consumer loop
# ==========================================

print("===================================")
print("IDEMPOTENT CONSUMER STARTED")
print("===================================")

for message in consumer:

    message_id = message.value

    print(
        f"\n[RECEIVED] "
        f"message={message_id} "
        f"offset={message.offset}"
    )

    process_message(message_id)

    # Commit Kafka offset AFTER business processing
    consumer.commit()

    print(
        f"[KAFKA COMMIT] "
        f"offset={message.offset + 1}"
    )
