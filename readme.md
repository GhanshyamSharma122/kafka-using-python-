# **Kafka Using Python — Minimal Kafka Broker Implementation**

A lightweight, educational implementation of Kafka’s core concepts — built in **Python** to make the internals easy to understand and reuse.
This project acts as a **mini Kafka broker**, demonstrating how logs, partitions, offsets, producers, and consumers work under the hood.

If you're exploring distributed systems or want to understand Kafka deeply (without running a full Kafka cluster), this project is for you.

---

## 🚀 **Why This Project?**

Real Kafka is powerful — but also heavy to run and hard to inspect internally.
This project focuses on **learning the internals**, showing concepts such as:

* Write-ahead logs
* Segmented log storage
* Offsets & message ordering
* Producer & consumer API behavior
* Record fetching with byte limits
* Topic-level partition logs

All implemented in clear, readable Python.

It’s also reusable as a plug-in message queue for custom apps or prototypes.

---

## 📂 **Project Structure**

```
.
├── broker.py        # Flask server acting as the Kafka broker
├── partition_log.py # Log segment & storage mechanics
├── producer.js      # Node client: sends messages
├── consumer.js      # Node client: fetches messages
└── broker_data/     # Auto-generated topic log directories
```

---

## ⚙️ **How to Use**

### **1. Install dependencies**

```bash
pip install Flask
npm install
```

### **2. Start the broker**

```bash
python broker.py
```

### **3. Start a producer**

```bash
node producer.js
```

### **4. Start a consumer**

```bash
node consumer.js
```

You now have a working minimal Kafka-like system producing and consuming messages with real offsets and persisted logs.

---

## 🧩 **API Overview**

### **Produce**

**POST** `/produce`

```json
{
  "topic": "my-topic",
  "payload": "Hello World"
}
```

Response:

```json
{ "offset": 42 }
```

### **Fetch**

**GET** `/fetch?topic=my-topic&offset=0&max_bytes=4096`

Response:

```json
{
  "records": [
    { "offset": 0, "payload": "Hello World" }
  ]
}
```

---

## 🔧 **Using This in Your Own App**

This system can act as a tiny message queue inside your custom app:

* Embed `PartitionLog` for high-speed append-only logs
* Use `produce`/`fetch` endpoints as an internal message bus
* Prototype distributed systems concepts without running Kafka

---

## 🌟 **Support & Follow**

If you enjoy projects like this and want more deep dives into backend systems, distributed architectures, and real-world implementations—

👉 **Follow me for more explorations and cool system design projects!**
