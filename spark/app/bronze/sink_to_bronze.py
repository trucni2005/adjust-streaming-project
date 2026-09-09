import base64
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DELTA_TABLE_PATH = os.getenv("BRONZE_DELTA_PATH", "/data/lakehouse/bronze_events")

KAFKA_BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
KAFKA_TOPIC = "adjust-dbserver.adjust.event"
# Spark tracks its own progress via checkpointLocation and never commits offsets
# back to Kafka, so lag-monitoring tools (Kafka UI, kafka-consumer-groups.sh)
# see nothing for this query by default. The listener below mirrors each
# micro-batch's end offset into this consumer group purely so those tools can
# show lag — it has no effect on the query's own recovery/correctness.
# Order matches the canonical Adjust event schema, plus a trailing event_date
# used only as the Delta partition column.
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
BOOL_COLUMNS = {"impression_based", "is_organic"}
LONG_COLUMNS = {"id", "__debezium_ts_ms"}


def _column_type(column):
    if column in LONG_COLUMNS:
        return LongType()
    if column in TIMESTAMP_COLUMNS:
        return TimestampType()
    if column in DECIMAL_COLUMNS:
        return DecimalType(18, 6)
    if column in BOOL_COLUMNS:
        return BooleanType()
    return StringType()


PARSED_SCHEMA = StructType(
    [StructField(column, _column_type(column)) for column in COLUMNS]
    + [StructField("event_date", StringType())]
)


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
    parsed["event_date"] = datetime.fromtimestamp(
        payload["ts_ms"] / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d")
    return parsed


def write_batch_to_delta(batch_df, batch_id):
    parsed_rdd = batch_df.select("value").rdd.map(lambda row: parse_message(row.value))
    parsed_rdd = parsed_rdd.filter(lambda row: row is not None).map(lambda row: Row(**row))
    if parsed_rdd.isEmpty():
        return

    parsed_df = spark.createDataFrame(parsed_rdd, schema=PARSED_SCHEMA)
    parsed_df.write.format("delta") \
        .mode("append") \
        .partitionBy("event_date") \
        .save(DELTA_TABLE_PATH)


spark = SparkSession.builder \
    .appName("SinkToBronze") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("kafka.group.id", "spark-streaming-consumer") \
    .load() \
    .selectExpr("CAST(value AS STRING) as value")

query = df.writeStream \
    .foreachBatch(write_batch_to_delta) \
    .trigger(processingTime="1 minutes") \
    .option("checkpointLocation", "/data/checkpoints/sink_to_bronze") \
    .start()

query.awaitTermination()

spark.stop()
