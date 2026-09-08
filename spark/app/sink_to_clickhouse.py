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
CLICKHOUSE_TABLE = "spark__events"

KAFKA_BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
KAFKA_TOPIC = "adjust-dbserver.adjust.event"
# Spark tracks its own progress via checkpointLocation and never commits offsets
# back to Kafka, so lag-monitoring tools (Kafka UI, kafka-consumer-groups.sh)
# see nothing for this query by default. The listener below mirrors each
# micro-batch's end offset into this consumer group purely so those tools can
# show lag — it has no effect on the query's own recovery/correctness.
# Order matches the column order in ddl/clickhouse/init.sql (flink__events / spark__events).
COLUMNS = [
    "id", "activity_kind", "created_at", "app_token", "store_id", "app_name", "app_version",
    "platform", "environment", "sdk_version", "os_name", "os_version", "device_type",
    "device_model", "language", "country", "country_subdivision", "city", "timezone",
    "adid", "gps_adid", "idfa", "idfv", "tracker", "tracker_name", "network_name",
    "campaign_name", "adgroup_name", "creative_name", "event", "event_name",
    "revenue_float", "currency", "reporting_revenue", "reporting_currency",
    "ad_impressions_count", "ad_mediation_platform", "ad_revenue_network",
    "ad_revenue_placement", "ad_revenue_unit", "subscription_event_type",
    "subscription_purchased_at", "subscription_expiration_time", "subscription_cancelled_at",
    "subscription_transaction_id", "subscription_original_transaction_id",
    "subscription_product_id", "subscription_sales_region", "reporting_cost",
    "installed_at", "click_time", "impression_time", "engagement_time",
    "impression_based", "is_organic", "op", "__debezium_ts_ms",
]
TIMESTAMP_COLUMNS = {
    "created_at", "subscription_purchased_at", "subscription_expiration_time",
    "subscription_cancelled_at", "installed_at", "click_time", "impression_time",
    "engagement_time",
}
DECIMAL_COLUMNS = {"revenue_float", "reporting_revenue", "ad_impressions_count", "reporting_cost"}


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

    parsed = {}
    for column in COLUMNS:
        if column == "op":
            parsed[column] = payload["op"]
        elif column == "__debezium_ts_ms":
            parsed[column] = payload["ts_ms"]
        elif column in TIMESTAMP_COLUMNS:
            epoch_micros = row.get(column)
            parsed[column] = (
                datetime.fromtimestamp(epoch_micros / 1_000_000, tz=timezone.utc)
                if epoch_micros is not None else None
            )
        elif column in DECIMAL_COLUMNS:
            parsed[column] = decode_debezium_decimal(row.get(column), 6)
        else:
            parsed[column] = row.get(column)
    return parsed


def write_partition_to_clickhouse(rows_iter):
    rows = [parse_message(row.value) for row in rows_iter]
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


def write_batch_to_clickhouse(batch_df, batch_id):
    batch_df.select("value").foreachPartition(write_partition_to_clickhouse)


spark = SparkSession.builder \
    .appName("SinkToClickhouse") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("kafka.group.id", "spark-streaming-consumer") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")

query = df.writeStream \
    .foreachBatch(write_batch_to_clickhouse) \
    .trigger(processingTime="1 minutes") \
    .option("checkpointLocation", "/data/checkpoints/sink_to_clickhouse") \
    .start()

query.awaitTermination()

spark.stop()
