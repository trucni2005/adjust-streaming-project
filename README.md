# Adjust Streaming Project

Pipeline dữ liệu streaming mô phỏng sự kiện attribution kiểu Adjust (install, ad revenue, subscription), lưu vào Postgres, đẩy CDC qua Kafka/Debezium, rồi Spark Structured Streaming ghi vào bảng Iceberg (bronze layer).

## Kiến trúc

```
simulator (FastAPI) --> Postgres (schema adjust) --> Debezium (kconnect) --> Kafka cluster (3 broker) --> Spark Structured Streaming --> Iceberg (bronze_events, trên MinIO/S3A)
```

- **simulator** — FastAPI service sinh sự kiện install/ad-revenue/subscription ngẫu nhiên, ghi vào bảng `adjust.event` trong Postgres.
- **postgres** — lưu raw event, chạy với `wal_level=logical` để Debezium đọc được WAL. Config ở [config/postgres/postgresql.conf](config/postgres/postgresql.conf); simulator và Debezium dùng user riêng theo quyền tối thiểu (tạo bởi [ddl/postgresql/02-roles.sh](ddl/postgresql/02-roles.sh)), superuser chỉ dùng cho quản trị.
- **kafka1/kafka2/kafka3** — cluster Kafka 3 broker chế độ KRaft (không dùng ZooKeeper).
- **kconnect** (Debezium) — bắt thay đổi từ Postgres qua CDC, publish vào topic Kafka `adjust-dbserver.adjust.event`. Connector không tự đăng ký khi start, xem [Setup](#setup) bước 4.
- **kafka-ui** ([kafbat](https://github.com/kafbat/kafka-ui)) — UI xem topic Kafka, schema và trạng thái connector Debezium; đăng nhập bằng `KAFKA_UI_USER`/`KAFKA_UI_PASSWORD`.
- **minio** — object storage tương thích S3, dùng làm Iceberg warehouse (`s3a://lakehouse`) và nơi lưu checkpoint Spark Structured Streaming (`s3a://data/checkpoints/...`).
- **minio-init** — service chạy 1 lần lúc `docker compose up`, tạo sẵn 2 bucket `lakehouse` và `data` trên MinIO (idempotent, an toàn khi chạy lại).
- **spark-master / spark-worker-1 / spark-worker-2** — chạy job Spark Structured Streaming ([spark/bronze/sink_to_bronze.py](spark/bronze/sink_to_bronze.py)) đọc từ Kafka, ghi (append-only) vào bảng Iceberg `bronze_catalog.<env>.bronze_events`, partition theo `event_date`. Thư mục `spark/silver` và `spark/gold` hiện chưa có job nào (placeholder).
- **spark-history-server** — Spark History Server, đọc event log để xem lại các job đã chạy.

### Kiến trúc tầng Gold

Bronze lưu raw event append-only; Silver làm sạch/conform; Gold là các bảng tổng hợp phục vụ trực tiếp câu hỏi nghiệp vụ (revenue, install, campaign performance...), theo mô hình star-schema rút gọn:

```mermaid
flowchart TB
    Bronze["Bronze (append-only)
bronze_catalog.&lt;env&gt;.bronze_events"]

    Silver["Silver (conform/clean)
dedupe theo id, cast type, chuẩn hóa timestamp
silver_catalog.&lt;env&gt;.conformed_adjust_events"]

    DimApp["dim_app (upsert batch)
app_token, app_name, platform..."]

    DimCampaign["dim_campaign (upsert batch)
tracker, network_name, campaign_name..."]

    FactDaily["fact_*_daily (window agg + MERGE INTO)
fact_installs_daily
fact_revenue_daily
fact_ad_revenue_daily
fact_subscription_daily
fact_events_daily"]

    Gold["gold_catalog.&lt;env&gt;.*
(Iceberg, MinIO/S3A)"]

    Bronze -->|readStream iceberg| Silver
    Silver -->|readStream iceberg| DimApp
    Silver -->|readStream iceberg| DimCampaign
    Silver -->|readStream iceberg| FactDaily
    DimApp --> Gold
    DimCampaign --> Gold
    FactDaily --> Gold
```

- **Silver** — một job streaming đọc lại Bronze qua Iceberg (`spark.readStream.format("iceberg")`), dedupe theo `id`, ép kiểu, chuẩn hóa timestamp; ghi append vào bảng Iceberg riêng, vẫn partition theo `event_date`.
- **Gold — dimension tables** (`dim_app`, `dim_campaign`) — ít thay đổi, cập nhật dạng upsert (`foreachBatch` + Iceberg `MERGE INTO`) từ Silver.
- **Gold — fact tables** (`fact_*_daily`) — đọc Silver streaming, `groupBy(window(...), keys).agg(...)` với watermark, ghi qua `foreachBatch` + `MERGE INTO` (không dùng `outputMode("append").toTable()` như bronze, vì mỗi micro-batch có thể cập nhật lại số liệu của cùng một ngày).
- Việc chạy các job này được điều phối qua `CONFIG_JOBS` trong [spark/spark_builder.py](spark/spark_builder.py) — mỗi phần tử `layer.job` trỏ tới một `config.yaml` + module `run(spark, config)` tương ứng trong `spark/<layer>/`.

## Cấu trúc Docker Compose

[docker-compose.yml](docker-compose.yml) chỉ là entry point, dùng `include:` để gộp các file trong [compose/](compose/) (đường dẫn và `.env` vẫn resolve từ thư mục gốc repo):

| File | Services |
|---|---|
| [compose/postgres.yml](compose/postgres.yml) | postgres |
| [compose/tooling-db.yml](compose/tooling-db.yml) | tooling-db |
| [compose/kafka.yml](compose/kafka.yml) | kafka1/2/3, schema-registry, kconnect, kafka-ui |
| [compose/storage.yml](compose/storage.yml) | minio, minio-init, iceberg-rest |
| [compose/spark.yml](compose/spark.yml) | spark-master, spark-worker-1/2, spark-history-server |
| [compose/bi.yml](compose/bi.yml) | trino, metabase |
| [compose/simulator.yml](compose/simulator.yml) | simulator |

### Network

- **backend-net** — vùng OLTP: `postgres`, `simulator` (đóng vai backend app). Mô phỏng thực tế Postgres thuộc network của backend, tách khỏi data platform.
- **kafka-net** — data platform: Kafka, Schema Registry, Spark, MinIO, Trino, Metabase...
- **kconnect** là service duy nhất gắn vào cả hai network (đọc WAL từ Postgres, ghi sang Kafka). Spark/Trino/Metabase... không kết nối được tới Postgres.

## Setup

1. Copy `.env.example` thành `.env` và điền giá trị (gồm cả password Postgres `PG_PASSWORD`, `PG_DEBEZIUM_PASSWORD` và credentials MinIO: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_ENDPOINT`). Password superuser Postgres nằm ở `secrets/pg_superuser_password` (không commit), `make postgres-up` tự sinh nếu chưa có — phải có file này trước khi chạy `docker compose up` trực tiếp.
2. `make up`
3. Init DDL ([ddl/postgresql/](ddl/postgresql/)) tự chạy khi volume `postgres-db-volumes` còn trống. Đổi password/quyền trong `.env` sau đó thì chạy `make postgres-roles`.
4. `make register-connector` để đăng ký connector Debezium (không persist — phải chạy lại sau khi teardown/wipe volume). Kiểm tra: `make connector-status` hoặc xem trong Kafka UI (http://localhost:8086 → Kafka Connect → debezium). Password Debezium không nằm trong config connector mà đọc từ `secrets/debezium.properties` (`make kafka-up` sinh từ `PG_DEBEZIUM_PASSWORD`).
5. Submit job Spark: `make spark-submit SPARK_LAYER=bronze SPARK_JOB=sink_to_bronze` (đọc credentials MinIO từ `.env`, truyền vào container qua `-e` lúc exec).

## Ports

| Service | Port | Ghi chú |
|---|---|---|
| Postgres | 5434 | map vào 5432 trong container, chỉ bind `127.0.0.1` |
| Kafka (broker 1/2/3) | — | không publish ra host; client trong `kafka-net` dùng `kafka1:9092,kafka2:9092,kafka3:9092`. Debug: Kafka UI hoặc `docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:9092 ...` |
| Schema Registry | 8081 | chỉ bind `127.0.0.1` |
| Kafka Connect (Debezium) | 8083 | chỉ bind `127.0.0.1` |
| Kafka UI | 8086 | chỉ bind `127.0.0.1`, cần đăng nhập |
| Simulator (FastAPI) | 8000 | |
| Spark master UI | 7070 | map vào 8080 trong container |
| Spark master RPC | 7077 | |
| Spark driver UI | 4040 | chỉ lên khi có job đang chạy |
| Spark worker 1 UI | 8091 | |
| Spark worker 2 UI | 8092 | |
| Spark History Server | 18080 | |
| MinIO API | 9000 | S3-compatible endpoint |
| MinIO Console | 9001 | user/pass = `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` |
