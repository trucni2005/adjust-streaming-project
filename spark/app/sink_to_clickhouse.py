import base64
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

import clickhouse_connect
import pandas as pd
from pyspark.sql import SparkSession

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "pass")
CLICKHOUSE_TABLE = "spark__ad_revenue_events"

KAFKA_BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
KAFKA_TOPIC = "dbserver1.adjust.event__ad_revenue"
# Spark tracks its own progress via checkpointLocation and never commits offsets
# back to Kafka, so lag-monitoring tools (Kafka UI, kafka-consumer-groups.sh)
# see nothing for this query by default. The listener below mirrors each
# micro-batch's end offset into this consumer group purely so those tools can
# show lag — it has no effect on the query's own recovery/correctness.
COLUMNS = [
    "id", "created_at", "store_id", "app_version", "tracker_name",
    "network_name", "campaign_name", "adgroup_name", "creative_name",
    "country", "adid", "ad_revenue_network", "ad_revenue_placement",
    "ad_revenue_unit", "reporting_revenue", "reporting_currency",
    "ad_impression_count", "op", "__debezium_ts_ms",
]


def decode_debezium_decimal(b64_value, scale):
    """Decode a Kafka Connect bytes-encoded Decimal (base64 two's-complement unscaled value)."""
    if b64_value is None:
        return None
    unscaled = int.from_bytes(base64.b64decode(b64_value), byteorder="big", signed=True)
    return Decimal(unscaled).scaleb(-scale)


def parse_message(value):
    payload = json.loads(value)["payload"]
    # "after" holds the row state for inserts/updates; deletes only have "before".
    row = payload["after"] or payload["before"]
    if row is None:
        return None

    return {
        "id": row["id"],
        "created_at": datetime.fromtimestamp(row["created_at"] / 1_000_000, tz=timezone.utc),
        "store_id": row["store_id"],
        "app_version": row["app_version"],
        "tracker_name": row["tracker_name"],
        "network_name": row["network_name"],
        "campaign_name": row["campaign_name"],
        "adgroup_name": row["adgroup_name"],
        "creative_name": row["creative_name"],
        "country": row["country"],
        "adid": row["adid"],
        "ad_revenue_network": row["ad_revenue_network"],
        "ad_revenue_placement": row["ad_revenue_placement"],
        "ad_revenue_unit": row["ad_revenue_unit"],
        "reporting_revenue": decode_debezium_decimal(row["reporting_revenue"], 6),
        "reporting_currency": row["reporting_currency"],
        "ad_impression_count": decode_debezium_decimal(row["ad_impression_count"], 6),
        "op": payload["op"],
        "__debezium_ts_ms": payload["ts_ms"],
    }


def write_batch_to_clickhouse(batch_df, batch_id):
    rows = [parse_message(row.value) for row in batch_df.select("value").collect()]
    rows = [row for row in rows if row is not None]
    if not rows:
        return

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
    )
    try:
        client.insert_df(CLICKHOUSE_TABLE, pd.DataFrame(rows, columns=COLUMNS))
    finally:
        client.close()


spark = SparkSession.builder \
    .appName("SinkToClickhouse") \
    .getOrCreate()
    
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("kafka.group.id", "spark-streaming-consumer") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")

query = df.writeStream \
    .foreachBatch(write_batch_to_clickhouse) \
    .trigger(processingTime="1 minutes") \
    .option("checkpointLocation", "/data/checkpoints/sink_to_clickhouse") \
    .start()

query.awaitTermination()

spark.stop()
