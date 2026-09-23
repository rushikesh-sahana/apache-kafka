# Kafka Producer-Consumer POC (Python)

A minimal, working example to learn Kafka by running it. One script publishes
fake "order" events, another reads them.

## 1. Start Kafka

You need Docker installed. This starts a single-node Kafka broker (no
Zookeeper needed — it uses modern KRaft mode).

```bash
docker compose up -d
```

Give it ~10-15 seconds to fully start. Check logs if unsure:

```bash
docker compose logs -f kafka
```

## 2. Install Python dependencies

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## 3. Run the consumer (in one terminal)

```bash
python consumer.py
```

It will sit and wait for messages.

## 4. Run the producer (in another terminal)

```bash
python producer.py
```

You'll see it publish a fake order every second, and the consumer terminal
will print each one as it arrives, along with its partition/offset.

Stop either with `Ctrl+C`. Stop Kafka with `docker compose down` (add `-v`
to also wipe the stored data).

---

## Kafka Terminology Cheat Sheet

| Term | Meaning |
|---|---|
| **Broker** | A single Kafka server. It stores data and serves producer/consumer requests. A "cluster" is one or more brokers working together. |
| **Topic** | A named category/feed that messages are published to — like a table name or a channel. In this POC: `"orders"`. |
| **Partition** | A topic is split into partitions for scalability and parallelism. Each partition is an ordered, append-only log. Order is guaranteed *within* a partition, not across the whole topic. |
| **Offset** | A message's position number within a partition (0, 1, 2, ...). Consumers track offsets to know what they've already read. |
| **Producer** | An application that writes/publishes messages to a topic (`producer.py` here). |
| **Consumer** | An application that reads/subscribes to messages from a topic (`consumer.py` here). |
| **Consumer Group** | A set of consumers sharing a `group_id` that cooperatively read a topic. Kafka splits the topic's partitions across the group members, so each message is processed by only one consumer *in that group*. Multiple groups can each independently read the whole topic. |
| **Key** | Optional bytes attached to a message. Kafka hashes the key to decide which partition the message goes to — same key always lands on the same partition, which preserves order for that key (e.g. all events for one `order_id`). |
| **Value** | The actual message payload/content (here, a JSON object). |
| **Serializer / Deserializer** | Converts your Python object to bytes to send (serializer) and bytes back to a Python object on read (deserializer), since Kafka only moves raw bytes. |
| **Replication Factor** | How many broker copies of each partition exist, for fault tolerance. Not really visible in this single-broker POC, but critical in production clusters. |
| **Retention** | How long Kafka keeps messages on a topic (time- or size-based) before deleting them, regardless of whether they've been consumed. |
| **Commit (offset commit)** | A consumer telling Kafka "I've successfully processed up to this offset." Lets it resume correctly after a restart instead of reprocessing or skipping messages. |

## What to try next
- Run **two** consumer processes with the same `group_id` while the producer
  is running — watch how they split the partitions between them (needs a
  multi-partition topic).
- Change `group_id` in `consumer.py` and rerun it — since it's a *new* group,
  it will re-read all retained messages from the start (`auto_offset_reset="earliest"`).
- Look at Kafka's own tools inside the container, e.g.:
  ```bash
  docker exec -it kafka-poc kafka-topics.sh --list --bootstrap-server localhost:9092
  ```