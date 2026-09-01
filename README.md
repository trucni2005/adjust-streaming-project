# Adjust Streaming Project

A local streaming data pipeline that simulates Adjust-style mobile attribution events (installs, ad revenue), persists them to Postgres, streams changes via Kafka/Debezium CDC, and sinks them into ClickHouse through two interchangeable stream processors — Flink and Spark — for side-by-side comparison.

## Architecture

```
simulator (FastAPI) --> Postgres (adjust schema) --> Debezium (kconnect) --> Kafka cluster --> Flink / Spark --> ClickHouse
```

- **simulator** — FastAPI service that generates random install / ad-revenue events and writes them to Postgres.
- **postgres** — stores raw events in the `adjust` schema. Runs with `wal_level=logical` so Debezium can read the write-ahead log.
- **kafka1/kafka2/kafka3** — 3-broker KRaft-mode Kafka cluster (no ZooKeeper).
- **kconnect** (Debezium) — captures Postgres changes via CDC and publishes them to Kafka topics.
- **kafka-ui** — web UI for inspecting Kafka topics and the Debezium connector.
- **flink-jobmanager / flink-taskmanager** — PyFlink job ([flink/jobs/sink_to_clickhouse.py](flink/jobs/sink_to_clickhouse.py)) consuming ad-revenue events and writing to ClickHouse table `flink__ad_revenue_events`.
- **spark-master / spark-worker-1 / spark-worker-2** — Spark Structured Streaming job ([spark/app/sink_to_clickhouse.py](spark/app/sink_to_clickhouse.py)) doing the same, writing to `spark__ad_revenue_events`.
- **clickhouse** — analytical sink. Table DDL lives in [ddl/clickhouse/01-events.sql](ddl/clickhouse/01-events.sql) (apply manually via `clickhouse-client` — not auto-applied on startup).

Flink and Spark write to separate tables on purpose so both pipelines can run against the same Kafka topic at once without clobbering each other's output.

## Setup

1. Copy `.env.example` to `.env` and fill in the values (Postgres/ClickHouse credentials).
2. `docker compose up -d --build`
3. Apply the ClickHouse DDL once the `clickhouse` container is up:
   ```
   docker compose exec -T clickhouse clickhouse-client --user default --password <pass> --multiquery < ddl/clickhouse/01-events.sql
   ```
4. Submit a processing job (either or both):
   ```
   scripts/run_flink_job.sh
   scripts/run_spark_job.sh
   ```
   Both are long-running streaming jobs — the scripts block until you stop them.

Editing `flink/jobs/*.py` or `spark/app/*.py` takes effect immediately on the next job submission (both are bind-mounted into their containers) — no rebuild needed unless you change a dependency in the `Dockerfile`.

## Ports

| Service | Port | Notes |
|---|---|---|
| Postgres | 5434 | maps to container's 5432 |
| Kafka (broker 1/2/3) | 9092 / 9094 / 9096 | |
| Kafka Connect (Debezium) | 8083 | |
| Kafka UI | 8080 | |
| Simulator (FastAPI) | 8000 | |
| Spark master UI | 7070 | maps to container's 8080 |
| Spark master RPC | 7077 | |
| Spark driver UI | 4040 | only up while a Spark job is running |
| Flink JobManager UI | 8001 | maps to container's 8081 |
| ClickHouse HTTP | 8123 | |
| ClickHouse native protocol | 9000 | |
