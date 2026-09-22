KAFKA-LEARNING
============================================================

## What is Apache Kafka? 

** **
* **Apache Kafka is a distributed event-streaming platform used to move large amounts of data between applications in real time.** 
* **For example,** when you order a pizza through an app, Kafka might handle the flow of information between the order service, payment service, kitchen display system, delivery tracking, and customer notifications. Each piece of information flows through Kafka to reach exactly where it needs to go.


## Common Use Cases for Apache Kafka

### 1. Real-time analytics / event streaming ###
* A ride-sharing app (like Uber) streams every driver's GPS location to Kafka, and downstream consumers calculate ETAs, surge pricing, and live maps in real time.  
### 2. Log aggregation ###
* Instead of each of 100 microservices writing logs to separate files, they all publish logs to Kafka topics, which are then consumed by a centralized system like Elasticsearch/Splunk for search and monitoring.
### 3. Website activity tracking ###
* LinkedIn (Kafka's original creator) uses it to capture user clicks, page views, and searches, feeding recommendation engines and analytics dashboards.
### 4. IoT data ingestion ##
* A real-world example of Kafka in IoT is a smart factory. Sensors attached to machines continuously generate temperature, vibration, pressure, and power data. These events are sent to Kafka topics. Different consumers can then process the same data—for example, one service displays real-time machine status, another stores it in a database, and another detects abnormal vibration and sends an alert to the maintenance team. Kafka acts as the reliable, scalable event-streaming layer between the IoT devices and these applications.
### 5. Using Kafka in a Microservices Architecture ###
* Scenario: An online store has separate microservices for Orders, Inventory, Payment, Shipping, and Notifications. Without Kafka, the Order Service would need to call each of these services directly (tight coupling, cascading failures if one is down). With Kafka, it just publishes one event.


# How Does Kafka Work?

Kafka operates on a **publish-subscribe model**, similar to how you might subscribe to a YouTube channel. Content creators (publishers) upload videos, and subscribers receive notifications about new content.

In Kafka's world:
- Applications that **send** data are called **producers**.
- Applications that **read** data are called **consumers**.
- The data itself flows through **topics**, which are like categories or channels.

## Example: Social Media Platform

Let's say you're building a social media platform. When a user posts a photo:

1. The **producer** (your upload service) sends that event to a topic called `user-posts`.
2. Multiple **consumers** can subscribe to this topic:
    - One consumer updates the user's profile
    - Another sends notifications to followers
    - A third stores the post in a database for long-term storage

```
                                  ┌──────────────────────┐
                                  │  Update User Profile  │
                                  └──────────────────────┘

┌────────────────┐  user-posts   ┌──────────────────────┐
│  Upload Service │ ────────────►│   Kafka Topic:         │──► Notify Followers
│   (Producer)    │    event     │   "user-posts"         │
└────────────────┘               └──────────────────────┘

                                  └──────────────────────┘
                                  │  Store Post in DB      │
                                  └──────────────────────┘
```

## Why This Matters: Decoupling

The beauty of this system is **decoupling**:

- The **producer** doesn't need to know who will use the data — it just publishes to a topic.
- The **consumer** doesn't need to know where the data came from — it simply subscribes to topics it cares about.

This means you can add, remove, or scale services independently without changing the producer or other consumers.

## Summary

| Role | Responsibility |
|---|---|
| Producer | Sends/publishes data to a topic |
| Topic | Named channel that categorizes the data |
| Consumer | Subscribes to and reads data from a topic |

> Kafka lets services communicate through events instead of direct calls — keeping systems loosely coupled, scalable, and resilient.

# Understanding Kafka Components in Detail 
  
## Brokers
### What is a Broker?

A **Kafka broker** is a server that stores and serves data. Think of brokers as individual post offices in different cities. Each post office handles mail for its region, but together they form a complete postal network.

### Brokers in a Cluster

In a Kafka cluster, you typically have multiple brokers working together. If you have three brokers, and one fails, the other two continue operating. Your data remains safe and accessible.

## Example: E-Commerce Site

Imagine you're running an e-commerce site with three Kafka brokers:

| Broker   | Responsibility              |
|----------|------------------------------|
| Broker 1 | Product catalog updates      |
| Broker 2 | Order information             |
| Broker 3 | User activity logs            |

If **Broker 2** crashes, Kafka automatically redirects traffic to the remaining brokers, ensuring your order processing never stops.

### Key Takeaway

Brokers provide **fault tolerance** and **high availability** by distributing data storage and request handling across multiple servers. The failure of a single broker does not bring down the entire system — the cluster continues serving data seamlessly.

# Kafka Topics

## What is a Topic?

Topics are **named channels** where data is organized. Every message sent to Kafka goes into a specific topic. You can think of topics like different newspaper sections: sports, business, entertainment. Each section contains related articles.

## Example: Ride-Sharing App

In a ride-sharing app, you might have topics like:

- `ride-requests`
- `driver-locations`
- `payments`
- `customer-ratings`

Each topic contains events specific to that category.

## Why Topics Matter

Topics make it easy to organize your data streams:

- Want to analyze payment patterns? Subscribe to the `payments` topic.
- Building a feature to show nearby drivers? Subscribe to `driver-locations`.

## Visual: Topics and Partitions

```
Topic: "orders"
    │
    ├── Partition 0: [msg1, msg2, msg3, msg4, ...]
    ├── Partition 1: [msg5, msg6, msg7, msg8, ...]
    └── Partition 2: [msg9, msg10, msg11, msg12, ...]

Topic: "payments"
    │
    ├── Partition 0: [pay1, pay2, pay3, ...]
    └── Partition 1: [pay4, pay5, pay6, ...]
```

## Key Takeaway

Topics let you logically separate different types of events, so consumers can subscribe only to the data streams that matter to them — without sifting through unrelated messages.

# Kafka Partitions

## What is a Partition?

Here's where Kafka gets really clever. Each topic is divided into **partitions**, which are like multiple lanes on a highway. Instead of all cars using one lane, they spread across multiple lanes, making traffic flow faster.

## Parallel Processing

Partitions allow Kafka to process data in parallel. If your `order-events` topic has five partitions, five different consumers can read from it simultaneously, each handling one partition. This dramatically increases processing speed.

## Example: Online Store

Let's say your online store processes 10,000 orders per minute:

- With **1 partition**, a single consumer must process all 10,000 orders.
- With **10 partitions**, you can have 10 consumers, each handling 1,000 orders.

Your processing speed just increased tenfold.

## Ordering Guarantees

Partitions also provide ordering guarantees **within** each partition:

- Messages in Partition 1 maintain their order.
- Messages in Partition 2 maintain their order.
- There's **no guaranteed ordering between partitions**.

## Key Takeaway

Partitions are the key to Kafka's scalability. They let you parallelize consumption across multiple consumers while still preserving strict message order where it matters — within a single partition.

# Kafka Producer — How to Send Data to Kafka

A guide to sending data (messages/records) to Apache Kafka using a producer client.

## Table of Contents
- [What is a Kafka Producer](#what-is-a-kafka-producer)
- [Message (Record) Structure](#message-record-structure)
- [Prerequisites](#prerequisites)
- [Key Configuration](#key-configuration)
- [How Data Is Sent (Flow)](#how-data-is-sent-flow)
- [Sync vs Async Sending](#sync-vs-async-sending)
- [Partitioning](#partitioning)
- [Best Practices](#best-practices)

---

## What is a Kafka Producer

A **Kafka Producer** is a client application that publishes (writes) records to one or more Kafka **topics**. Producers connect to the Kafka cluster, serialize data, determine the target partition, and send it to the broker that leads that partition.

* **Producers** are applications that send data to Kafka topics. They decide which topic to send data to and, optionally, which partition within that topic.
*   **A weather** monitoring system is a good example. Sensors across a city continuously send temperature, humidity, and air quality readings. These sensors act as producers, publishing measurements to a “weather-data” topic every few seconds.
---

## Message (Record) Structure

Every record sent to Kafka has:

| Field | Required | Description |
|---|---|---|
| `topic` | Yes | Destination topic name |
| `value` | Yes | The actual payload (JSON, string, Avro, Protobuf, bytes, etc.) |
| `key` | No | Used for partition routing & ordering per key |
| `partition` | No | Explicit partition number (otherwise auto-assigned) |
| `headers` | No | Key-value metadata pairs |
| `timestamp` | No | Event or ingestion time |

Kafka stores everything as **bytes** — the producer's serializer converts your data into bytes before sending.

---

## Prerequisites

- A running Kafka broker (e.g. `localhost:9092`)
- A topic created on the cluster (or auto-creation enabled)
- A client library for your language of choice (Java client, `confluent-kafka`, `kafka-python`, `kafkajs`, etc.)

---

## Key Configuration

| Config | Purpose |
|---|---|
| `bootstrap.servers` | Broker address(es) to connect to |
| `key.serializer` / `value.serializer` | How to convert key/value into bytes |
| `acks` | Durability: `0` (none), `1` (leader only), `all` (all in-sync replicas) |
| `retries` | Retry attempts on transient failure |
| `batch.size` | Max bytes buffered per partition before sending |
| `linger.ms` | Max time to wait to fill a batch before sending |
| `compression.type` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `enable.idempotence` | Prevents duplicate writes on retry |

---

## Sync vs Async Sending

- **Async (recommended for throughput):** the producer sends the record and continues immediately, notifying the app later via a callback when the broker responds
- **Sync (for correctness-critical paths):** the app blocks and waits until the broker acknowledges the record before continuing

---

## Partitioning

- **With a key** → same key always lands on the same partition (via hashing), preserving order for that key
- **Without a key** → sticky/round-robin distribution across partitions

---

## Best Practices

- Use `acks=all` + `enable.idempotence=true` to avoid data loss and duplicates
- Tune `linger.ms` + `batch.size` together for better throughput
- Always flush the producer before shutting down to avoid losing buffered messages
- Use a meaningful **key** when order matters for related records (e.g. same user/entity ID)
- Handle serialization errors and delivery failures explicitly
- Prefer schema-based formats (Avro/Protobuf + Schema Registry) over raw JSON in production systems

---


## What are Consumers?
* **The Consumers:**
* Are client applications that 𝗿𝗲𝗮𝗱 𝗱𝗮𝘁𝗮 𝗳𝗿𝗼𝗺 𝗞𝗮𝗳𝗸𝗮 𝘁𝗼𝗽𝗶𝗰𝘀.
* Subscribe to one or more topics and reads the messages in the order in which they were produced to each partition.
* Keeps track of which messages it has already consumed by keeping track of the offset of messages.

## What is a Consumer Group?

* The group ensures that each partition is only consumed by one member.
* Consumer groups enable horizontal scalability. As your data volume grows, you can add more consumers to the group, and Kafka will automatically rebalance the partitions among the consumers.

### 𝗜𝗺𝗽𝗼𝗿𝘁𝗮𝗻𝘁: 
* When a topic receives new messages, Kafka distributes those messages across the partitions of the topic.
* Each consumer group is assigned a set of partitions to consume from.
* If a single consumer fails, the remaining members of the group will reassign the partitions being consumed to take over for the missing member.
* Consumer groups in Kafka are useful for scaling the processing of messages across multiple consumers.

## Offsets
* **Offsets** are sequential numbers assigned to each message in a partition. They work like page numbers in a book, allowing consumers to track their reading progress.
* When a consumer reads a message, it records the offset. If the consumer crashes and restarts, it knows exactly where to resume reading. No messages are skipped or processed twice.

## Offset Commit

* Offsets can be committed to Kafka either automatically or manually by the consumer. This is stored in Kafka’s internal topic called __consumer_offsets.
  — Automatic commit: Kafka commits the current offset periodically at an interval configured by the consumer (e.g., every 5 seconds).
  — Manual commit: The consumer explicitly commits the offset when certain conditions are met, giving more control over processing (e.g., after processing a batch of messages successfully).

## Retention Period

* Retention period is how long Kafka keeps messages in a topic before deleting them, regardless of whether consumers have read them. Kafka doesn't delete messages just because a consumer has read them — it keeps them until the retention limit is hit.

### Types of Retention
* Time-based retention (most common) default 7 days we can set 
* Size-based retention Sets a max size per partition. Once exceeded, oldest segments are deleted.
* Size-based Retention (1GB per partition):
* Partition grows to 1.2GB → Oldest messages deleted until size is 1GB

### Replication

* Replication is Kafka’s insurance policy. Each partition is copied to multiple brokers. If your replication factor is three, each partition exists on three different brokers.
  Kafka replication is how Kafka achieves fault tolerance and durability — data isn't lost even if a broker (server) goes down. Here's how it works:
* A Kafka topic is split into partitions, and each partition is replicated across multiple brokers in the cluster.
* when we give replication factor

## Leader & Followers
* Each partition has one leader and multiple followers
* All reads and writes go through the leader.
* Followers just replicate data from the leader — they don't serve client traffic directly (unless you enable follower fetching for reads, added in newer versions)
* If the leader broker dies, one of the followers is automatically promoted to leader.

### 