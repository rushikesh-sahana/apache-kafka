# Outbox Pattern POC: Python + Kafka + SQLite

A small proof of concept showing how to **never lose an event when Kafka is down**.

The service tries Kafka first (3 attempts). If Kafka still doesn't confirm, the event is
saved in an `outbox` table, and a scheduler re-sends it every 60 seconds.

---

## Tech stack

| Part | Tool |
|---|---|
| Language | Python |
| Kafka client | `kafka-python-ng==2.2.3` |
| Database | SQLite (`shop.db`, created automatically) |
| Kafka broker | Docker (`apache/kafka:3.7.0`) |

## Project structure

```
outbox-poc/
├── docker-compose.yml    # runs Kafka only
├── requirements.txt      # kafka-python-ng==2.2.3
├── db.py                 # SQLite connection + creates tables (orders, outbox)
├── kafka_client.py       # send_event (1 attempt) and send_with_retry (3 attempts)
├── order_service.py      # creates an order, sends event, falls back to outbox
├── outbox_scheduler.py   # every 60s, sends pending outbox events to Kafka
└── consumer.py           # reads events from Kafka (skips duplicates)
```

---

## The flow

### 1. Big picture

```
 ┌───────────────┐   1. save order    ┌──────────┐
 │ order_service │ ─────────────────► │  SQLite  │
 │               │                    │ (orders) │
 │               │                    │          │
 │               │  4. Kafka down?    │ (outbox) │
 │               │ ─ save event ────► │          │
 └───────┬───────┘                    └────┬─────┘
         │ 2. send event                   │ 5. read pending rows
         │    (3 attempts)                 │    every 60s
         ▼                                 ▼
    ┌─────────┐                     ┌───────────────────┐
    │  Kafka  │ ◄────────────────── │ outbox_scheduler  │
    └────┬────┘   6. send + mark    └───────────────────┘
         │           processed
         ▼
    ┌──────────┐
    │ consumer │
    └──────────┘
```

### 2. Order service flow

```
create_order("bob", 250)
        │
        ▼
Build order_id and event (with a unique event_id)
        │
        ▼
Save order in SQLite
        │
        ▼
Send event to Kafka  ── attempt 1 ──► fail? wait 1s
                     ── attempt 2 ──► fail? wait 1s
                     ── attempt 3 ──► fail?
        │                                  │
   any attempt OK                    all 3 failed
        │                                  │
        ▼                                  ▼
      Done                     Insert event into outbox
   (outbox not used)           (processed_at = NULL = pending)
```

### 3. Scheduler flow (runs every 60 seconds)

```
Wake up
   │
   ▼
Get oldest pending row  (processed_at IS NULL, ORDER BY id, LIMIT 1)
   │
   ├── none found ───────────────► sleep 60s
   │
   ▼
Send to Kafka (1 attempt)
   │
   ├── OK ──► set processed_at = now ──► get next pending row
   │
   └── FAIL ─► stop this run ──► sleep 60s ──► try again next run
```

### 4. Timeline: Kafka goes down and comes back

| Time | What happens |
|---|---|
| 10:00:00 | Kafka goes down |
| 10:00:10 | Order saved; 3 Kafka attempts fail (~10s) |
| 10:00:20 | Event saved in `outbox` as pending |
| 10:01:00 | Scheduler runs, Kafka still down, row stays pending |
| 10:01:30 | Kafka comes back |
| 10:02:00 | Scheduler runs, sends event, marks it processed |
| 10:02:00 | Consumer receives the event |

---

## How to run

Run everything from inside the `outbox-poc` folder.

**1. Create and activate a virtual environment**
```bash
python -m venv venv

# Windows (PowerShell)
venv\Scripts\Activate.ps1
# Mac / Linux
source venv/bin/activate
```

**2. Install dependencies**
```bash
pip install -r requirements.txt
```

**3. Start Kafka** (wait ~10 seconds)
```bash
docker compose up -d
```

**4. Start the three parts** (each in its own terminal, with the venv activated)
```bash
python consumer.py                 # terminal 1: prints received events
python outbox_scheduler.py         # terminal 2: resends pending events every 60s
python order_service.py bob 250    # terminal 3: creates an order
```

---

## Test scenarios

### A. Kafka is up (happy path)
```bash
python order_service.py bob 250
```
- Output: `Kafka OK (attempt 1)`
- The consumer prints the event immediately.
- The outbox stays empty.

### B. Kafka is down, then comes back
```bash
docker compose stop kafka
python order_service.py carol 100
```
- Output: 3 failed attempts, then `event saved to outbox`.
- Check the outbox:
  ```bash
  sqlite3 shop.db "select id, event_type, processed_at from outbox"
  ```
  `processed_at` is empty, so the event is pending.

```bash
docker compose start kafka
```
- Within a minute the scheduler prints `Sent 1 pending event(s)`.
- The consumer prints carol's event.
- `processed_at` now has a timestamp.

---

## Key concepts

| Concept | Meaning |
|---|---|
| **Outbox table** | A table that holds events that still need to reach Kafka |
| **Pending row** | An outbox row with `processed_at` empty |
| **`event_id`** | Unique id of an event; the same id is used whether it is sent now or later |
| **At-least-once delivery** | An event is never lost, but it can occasionally arrive twice |
| **Idempotent consumer** | The consumer skips an `event_id` it has already seen |

## Limitations (good to know)

- **Small gap:** if the app crashes after saving the order but before writing the outbox row, that event is lost. The strict outbox pattern avoids this by inserting the order and the outbox row in the same transaction, then sending from the outbox.
- **Run only one scheduler.** SQLite has no row-level locking, so two schedulers could send the same event twice.
- **Duplicates are possible** (for example, Kafka stores a message but the ack is lost). The consumer handles this with `event_id`.
- **Sending one row at a time** is simple, but slower than batching if there are thousands of pending events.
- The consumer keeps seen ids in memory. In real systems, store them in a database.

## Handy commands

```bash
docker compose ps                  # is Kafka running?
docker compose stop kafka          # simulate Kafka down
docker compose start kafka         # bring Kafka back
docker compose down                # stop and remove containers
sqlite3 shop.db "select * from outbox"
sqlite3 shop.db "select * from orders"
```