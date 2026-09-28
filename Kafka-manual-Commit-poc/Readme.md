# Kafka Manual Commit POC (FastAPI + aiokafka)

Demonstrates `enable_auto_commit=False` and the real production incidents
that show up depending on *where* you place the commit call relative to
your business logic.

## Run it

```bash
docker compose up -d          # starts Kafka (KRaft, single broker) + Kafka UI on :8080
pip install -r requirements.txt

# terminal 1
uvicorn app.main:app --reload --port 8000

# terminal 2 — seed messages
python producer.py --count 30
```

Open http://localhost:8000/docs to drive it, or use curl:

```bash
curl -X POST localhost:8000/scenario -H "Content-Type: application/json" \
     -d '{"scenario": "crash_after_process"}'

curl localhost:8000/status

curl -X POST localhost:8000/consumer/restart   # simulates a pod restart

curl localhost:8000/status                     # watch side_effects jump vs processed_count
```

## Scenarios and what they simulate in production

| Scenario | Commit placement | What happens in prod | Guarantee |
|---|---|---|---|
| `safe` | after processing succeeds | correct baseline — a crash mid-processing just re-delivers that one message | at-least-once |
| `commit_before_process` | before processing | crash/exception between commit and processing = **message silently lost**, offset already moved past it | at-most-once (data loss) |
| `crash_after_process` | never (simulated crash) | side effect (charge, email, DB write) already happened, but offset not committed → on restart the **same message is re-delivered and the side effect fires again** | at-least-once (duplicates) |
| `slow_processing` | after a 15s sleep | handler runs longer than `max_poll_interval_ms` → broker's group coordinator marks the consumer dead → **rebalance**, then `commit()` raises `CommitFailedError` because the partition was reassigned mid-flight | rebalance storm / zombie consumer |
| `batch_commit` | once per N messages | crash mid-batch after e.g. 3 of 5 processed but 0 committed → **all previously "done" messages in that batch replay** on restart, not just the last one | amplified duplicate blast radius |
| `off_by_one_commit` | `msg.offset` instead of `msg.offset + 1` | Kafka's commit contract is "next offset to read." Committing the current offset means that **exact message is fetched again forever** | silent permanent duplicate leak |

## Why this matters (the actual production lessons)

1. **Kafka only guarantees you the message; it never guarantees "exactly
   once" processing for free.** Manual commit just lets you choose which
   failure mode you get — loss or duplication — you can't eliminate both
   without extra machinery.
2. **Commit after the side effect, never before**, if you care about not
   losing data (`commit_before_process` is the classic silent-data-loss bug).
3. **Any at-least-once consumer must be idempotent**, or paired with an
   idempotency key / outbox table / dedup store — because
   `crash_after_process` and `batch_commit` are not edge cases, they are the
   normal behavior of a distributed system with crashes and restarts.
4. **Keep processing time comfortably under `max_poll_interval_ms`.** If a
   handler can be slow (network calls, big batch, retries), either shrink
   the batch (`max.poll.records`), do the slow work off the poll thread, or
   raise the timeout deliberately — otherwise you get unexpected rebalances
   and `CommitFailedError` under load, which is a very common on-call page.
5. **Batch commits trade throughput for blast radius.** Committing less
   often is faster but means more reprocessing (and more duplicate side
   effects) per crash. Size the batch to what you can safely re-run.
6. **Off-by-one on offsets is a real, subtle bug class.** `commit(offset)`
   means "resume at `offset`", so you must always commit
   `message.offset + 1`, not `message.offset`.

## Mitigations used in real systems

- **Idempotency keys** on the consumer side (e.g. `order_id` in a `UNIQUE`
  DB constraint, or a Redis `SETNX` dedup cache) so re-delivery is a no-op.
- **Transactional outbox / Kafka transactions** (`read_process_write`) when
  you need the DB write and the offset commit to be atomic.
- **Idempotent producers + `acks=all`** upstream so the topic itself isn't
  the source of duplicates.
- **Alerting on rebalance frequency and consumer lag**, since both are
  early signals of the `slow_processing` scenario happening in production.
- **Dead-letter topics** for messages that repeatedly fail, so a poison
  message doesn't block the partition or trigger endless reprocessing.

## Classic failure scenario (most common problem):

* Consumer polls messages 100–199
* Processes message 150 successfully
* Crashes before calling commit()
* On restart → starts again from last committed offset (e.g. 100)
* Messages 100–149 are processed again → duplicates
- **Solution Pattern** → At-Least-Once + Idempotent Processing 