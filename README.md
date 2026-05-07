\# Real-Time Cryptocurrency Streaming Pipeline with Kafka



This project implements a real-time streaming data pipeline using Kafka, Docker, and Python. 

Cryptocurrency trade events are ingested from the Binance WebSocket API, streamed through Kafka 

topics, processed with windowed aggregations, and persisted as JSON outputs.



\## Architecture

```mermaid

graph TD

&#x20;   A\[Binance WebSocket API] --> B\[Kafka Producer]

&#x20;   B --> C\[Kafka Topic: crypto-transaction]

&#x20;   C --> D\[Kafka Consumer]

&#x20;   D --> E\[Window Aggregations]

&#x20;   E --> F\[JSON Output Files]

```

\## Tech Stack



\- Python

\- Apache Kafka

\- Docker \& Docker Compose

\- Binance WebSocket API

\- kafka-python

\- Real-time streaming

\- Windowed event processing



\## Features



\- Real-time cryptocurrency trade ingestion

\- Kafka-based event streaming

\- Dockerized distributed architecture

\- Windowed aggregations

\- JSON persistence layer

\- Graceful shutdown handling

\- Consumer group processing

\- Fault-tolerant container restart



\## Project Structure

.

├── producer.py

├── consumer\_windowed.py

├── docker-compose.yml

├── Dockerfile

├── requirements.txt

├── output/

└── README.md



\## How to run



```bash

git clone <repo-url>

cd kafka-pipeline

```

```bash

docker compose up --build

```

\## Open Kafka UI



Kafka UI:

http://localhost:8080



\## Example output

```json

{

&#x20; "timestamp": "2026-05-06T22:48:10",

&#x20; "window\_seconds": 10,

&#x20; "aggregations": {

&#x20;   "BTCUSDT": {

&#x20;     "avg\_price": 81302.14,

&#x20;     "min\_price": 81295.10,

&#x20;     "max\_price": 81310.50,

&#x20;     "event\_count": 245

&#x20;   }

&#x20; }

}

```

\## Future Improvements



\- Spark Structured Streaming integration

\- Delta Lake storage

\- Medallion Architecture

\- Real-time dashboards

\- Schema Registry

\- Dead-letter queue implementation

\- Exactly-once semantics

\- Kubernetes deployment





&#x20; !\[Kafka UI](images/kafka-ui.png)

