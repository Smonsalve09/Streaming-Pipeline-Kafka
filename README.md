# Real-Time Cryptocurrency Streaming Pipeline with Kafka

This project implements a real-time streaming data pipeline using Kafka, Docker, and Python.

Cryptocurrency trade events are ingested from the Binance WebSocket API, streamed through Kafka topics, processed with windowed aggregations, and persisted as JSON outputs.

---

## Architecture

```mermaid
graph TD
    A[Binance WebSocket API] --> B[Kafka Producer]
    B --> C[Kafka Topic: crypto-transaction]
    C --> D[Kafka Consumer]
    D --> E[Window Aggregations]
    E --> F[JSON Output Files]
```

---

## Tech Stack

* Python
* Apache Kafka
* Docker & Docker Compose
* Binance WebSocket API
* kafka-python
* Real-time streaming
* Windowed event processing

---

## Features

* Real-time cryptocurrency trade ingestion
* Kafka-based event streaming
* Dockerized distributed architecture
* Windowed aggregations
* JSON persistence layer
* Graceful shutdown handling
* Consumer group processing
* Fault-tolerant container restart

---

## Project Structure

```text
.
├── producer.py
├── consumer.py
├── output/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## How to Run

### Clone repository

```bash
git clone https://github.com/Smonsalve09/Streaming-Pipeline-Kafka.git
cd Streaming-Pipeline-Kafka
```

### Start services

```bash
docker compose up --build
```

---

## Open Kafka UI

Kafka UI is available at:

```text
http://localhost:8080
```

---

## Example Output

```json
{
  "timestamp": "2026-05-06T22:48:10",
  "window_seconds": 10,
  "aggregations": {
    "BTCUSDT": {
      "avg_price": 81302.14,
      "min_price": 81295.10,
      "max_price": 81310.50,
      "event_count": 245
    }
  }
}
```

---

## Future Improvements

* Spark Structured Streaming integration
* Delta Lake storage
* Medallion Architecture
* Real-time dashboards
* Schema Registry
* Dead-letter queue implementation
* Exactly-once semantics
* Kubernetes deployment

---

## Screenshots

<img width="1735" height="918" alt="image" src="https://github.com/user-attachments/assets/21a94272-60b4-4ac8-a380-11f03814fc58" />


<img width="1589" height="705" alt="image" src="https://github.com/user-attachments/assets/c0037c60-6e3a-48d1-b9a7-f741cdccfcfa" />

<img width="1595" height="650" alt="image" src="https://github.com/user-attachments/assets/06e2cfb4-0b0a-48ec-9366-f1cf328aa7e8" />

