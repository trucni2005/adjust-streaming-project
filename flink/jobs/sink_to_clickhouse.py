import base64
import json
import os
from datetime import datetime, timezone
from decimal import Decimal

from pyflink.common import Configuration, Row, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import MapFunction

BOOTSTRAP_SERVERS = "kafka1:9092,kafka2:9092,kafka3:9092"
EVENTS_TOPIC = "adjust-dbserver.adjust.event"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "pass")
CLICKHOUSE_TABLE = "flink__events"

# Order matches the column order in ddl/clickhouse/init.sql (flink__events / spark__events)
# so that the Flink JDBC sink, which binds "?" placeholders positionally, lines up correctly.
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

ROW_TYPE = Types.ROW([
    Types.LONG() if c in LONG_COLUMNS else
    Types.SQL_TIMESTAMP() if c in TIMESTAMP_COLUMNS else
    Types.BIG_DEC() if c in DECIMAL_COLUMNS else
    Types.BOOLEAN() if c in BOOL_COLUMNS else
    Types.STRING()
    for c in COLUMNS
])
INSERT_SQL = f"INSERT INTO {CLICKHOUSE_TABLE} ({', '.join(COLUMNS)}) VALUES ({', '.join('?' * len(COLUMNS))})"


def _to_timestamp(epoch_micros):
    if epoch_micros is None:
        return None
    return datetime.fromtimestamp(epoch_micros / 1_000_000, tz=timezone.utc)


def _numeric(b64_value, scale=6):
    """Debezium (decimal.handling.mode=precise, the default) encodes Postgres
    NUMERIC columns as base64-encoded unscaled BigDecimal bytes (Kafka Connect's
    org.apache.kafka.connect.data.Decimal wire format), not plain decimal strings."""
    if b64_value is None:
        return None
    unscaled = int.from_bytes(base64.b64decode(b64_value), byteorder="big", signed=True)
    return Decimal(unscaled).scaleb(-scale)


class DebeziumToEventRow(MapFunction):
    def map(self, raw: str) -> Row:
        payload = json.loads(raw).get("payload", {})
        after = payload.get("after") or payload.get("before") or {}
        values = {}
        for column in COLUMNS:
            if column == "op":
                values[column] = payload.get("op")
            elif column == "__debezium_ts_ms":
                values[column] = payload.get("ts_ms")
            elif column in TIMESTAMP_COLUMNS:
                values[column] = _to_timestamp(after.get(column))
            elif column in DECIMAL_COLUMNS:
                values[column] = _numeric(after.get(column))
            else:
                values[column] = after.get(column)
        return Row(*(values[c] for c in COLUMNS))


def main():
    config = Configuration()
    config.set_string("execution.checkpointing.interval", "1min")
    config.set_string("state.checkpoints.dir", "file:///opt/flink/checkpoints/sink_to_clickhouse/")

    env = StreamExecutionEnvironment.get_execution_environment(config)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(EVENTS_TOPIC)
        .set_group_id("flink-clickhouse-events")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    connection_options = (
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .with_url(f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/default")
        .with_driver_name("com.clickhouse.jdbc.ClickHouseDriver")
        .with_user_name(CLICKHOUSE_USER)
        .with_password(CLICKHOUSE_PASSWORD)
        .build()
    )
    execution_options = (
        JdbcExecutionOptions.builder()
        .with_batch_size(500)
        .with_batch_interval_ms(5000)
        .with_max_retries(3)
        .build()
    )

    env.from_source(source, WatermarkStrategy.no_watermarks(), "events-source").map(
        DebeziumToEventRow(), output_type=ROW_TYPE
    ).add_sink(JdbcSink.sink(INSERT_SQL, ROW_TYPE, connection_options, execution_options))

    env.execute("sink-events-to-clickhouse")


if __name__ == "__main__":
    main()
