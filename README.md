# Adjust Streaming Project

A local streaming data pipeline that simulates Adjust-style mobile attribution events (installs, ad revenue, subscriptions), persists them to Postgres, streams changes via Kafka/Debezium CDC, and processes them with a Spark Structured Streaming job that lands them in a Delta Lake table.

## Architecture

```
simulator (FastAPI) --> Postgres (adjust schema) --> Debezium (kconnect) --> Kafka cluster --> Spark Structured Streaming --> Delta Lake (bronze_events)
```

- **simulator** — FastAPI service that generates random install / ad-revenue / subscription events and writes them to a single `adjust.event` table in Postgres, distinguished by an `activity_kind` column.
- **postgres** — stores raw events in the `adjust` schema. Runs with `wal_level=logical` so Debezium can read the write-ahead log.
- **kafka1/kafka2/kafka3** — 3-broker KRaft-mode Kafka cluster (no ZooKeeper).
- **kconnect** (Debezium) — captures Postgres changes via CDC and publishes them to a single Kafka topic (`adjust-dbserver.adjust.event`). The connector isn't auto-registered on startup — see [Setup](#setup) step 3.
- **kafka-ui** — web UI for inspecting Kafka topics and the Debezium connector.
- **spark-master / spark-worker-1 / spark-worker-2** — Spark Structured Streaming job ([spark/app/bronze/sink_to_bronze.py](spark/app/bronze/sink_to_bronze.py)) consuming the topic and appending (insert-only, full CDC history kept) to a Delta Lake table at `/data/lakehouse/bronze_events` (bind-mounted host-side under `spark/data/lakehouse`), partitioned by `event_date`. A `spark/app/silver` directory exists as a placeholder for a follow-on silver-layer job, not yet implemented.
- **spark-history-server** — Spark History Server reading event logs written to `/data/spark-events` (bind-mounted host-side under `spark/data/spark-events`), so completed job runs can be inspected after the fact.

## Setup

1. Copy `.env.example` to `.env` and fill in the values (Postgres credentials).
2. `docker compose up -d --build`
3. Register the Debezium connector once `kconnect` is up (this is not persisted anywhere — redo it after a full stack teardown/volume wipe):
   ```
   curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d '{
       "name": "streaming-connector",
       "config": {
         "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
         "database.hostname": "postgres",
         "database.port": "5432",
         "database.user": "postgres",
         "database.password": "postgres",
         "database.dbname": "postgres",
         "database.server.name": "adjust-dbserver",
         "slot.name": "debezium",
         "plugin.name": "pgoutput",
         "table.include.list": "adjust.event"
       }
     }'
   ```
   This publishes changes to the `adjust.event` table on the `dbserver1.adjust.event` Kafka topic (Debezium's default `<database.server.name>.<schema>.<table>` naming). Check status with:
   ```
   curl http://localhost:8083/connectors/streaming-connector/status
   ```
   or inspect it in Kafka UI (http://localhost:8088 → Kafka Connect → debezium).
4. Submit the Spark processing job:
   ```
   scripts/run_spark_job.sh
   ```
   This is a long-running streaming job — the script blocks until you stop it.

Editing `spark/app/**/*.py` takes effect immediately on the next job submission (bind-mounted into the Spark containers) — no rebuild needed unless you change a dependency in the `Dockerfile`.

## Ports

| Service | Port | Notes |
|---|---|---|
| Postgres | 5434 | maps to container's 5432 |
| Kafka (broker 1/2/3) | 9092 / 9094 / 9096 | |
| Kafka Connect (Debezium) | 8083 | |
| Kafka UI | 8088 | |
| Simulator (FastAPI) | 8000 | |
| Spark master UI | 7070 | maps to container's 8080 |
| Spark master RPC | 7077 | |
| Spark driver UI | 4040 | only up while a Spark job is running |
| Spark worker 1 UI | 8091 | |
| Spark worker 2 UI | 8092 | |
| Spark History Server | 18080 | |
