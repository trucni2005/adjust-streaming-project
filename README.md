# Adjust Streaming Project

A local streaming data pipeline that simulates Adjust-style mobile attribution events (installs, ad revenue, subscriptions), persists them to Postgres, and streams changes via Kafka/Debezium CDC for downstream processing with Spark.

## Architecture

```
simulator (FastAPI) --> Postgres (adjust schema) --> Debezium (kconnect) --> Kafka cluster --> Spark
```

- **simulator** — FastAPI service that generates random install / ad-revenue events and writes them to Postgres.
- **postgres** — stores raw events in the `adjust` schema. Runs with `wal_level=logical` so Debezium can read the write-ahead log.
- **kafka1/kafka2/kafka3** — 3-broker KRaft-mode Kafka cluster (no ZooKeeper).
- **kconnect** (Debezium) — captures Postgres changes via CDC and publishes them to Kafka topics.
- **kafka-ui** — web UI for inspecting Kafka topics and the Debezium connector.
- **spark-master / spark-worker-1 / spark-worker-2** — Spark cluster for consuming and processing the event stream.

## Ports

| Service | Port |
|---|---|
| Postgres | 5434 |
| Kafka (broker 1/2/3) | 9092 / 9094 / 9096 |
| Kafka Connect (Debezium) | 8083 |
| Kafka UI | 8080 |
| Simulator (FastAPI) | 8000 |
| Spark master UI | 7070 |
| Spark master RPC | 7077 |
