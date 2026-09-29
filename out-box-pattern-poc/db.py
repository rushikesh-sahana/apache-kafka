"""Shared SQLite setup. sqlite3 is in the Python standard library."""
import sqlite3

DB_PATH = "shop.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")  # reader/writer don't block each other
    return conn


def init_db() -> None:
    conn = get_conn()
    with conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id         TEXT PRIMARY KEY,
                customer   TEXT NOT NULL,
                amount     REAL NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            -- Events are written here in the SAME transaction as the business data
            CREATE TABLE IF NOT EXISTS outbox (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                aggregate_id TEXT NOT NULL,
                event_type   TEXT NOT NULL,
                payload      TEXT NOT NULL,   -- JSON string
                created_at   TEXT NOT NULL DEFAULT (datetime('now')),
                processed_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed
                ON outbox (id) WHERE processed_at IS NULL;
            """
        )
    conn.close()