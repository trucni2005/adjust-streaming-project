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
AD_REVENUE_TOPIC = "dbserver1.adjust.event__ad_revenue"

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "pass")

COLUMNS = [
    "id", "created_at", "store_id", "app_version", "tracker_name",
    "network_name", "campaign_name", "adgroup_name", "creative_name",
    "country", "adid", "ad_revenue_network", "ad_revenue_placement",
    "ad_revenue_unit", "reporting_revenue", "reporting_currency",
    "ad_impression_count", "op", "__debezium_ts_ms",
]
ROW_TYPE = Types.ROW([
    Types.LONG(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.STRING(), Types.STRING(),
    Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
    Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
    Types.STRING(), Types.BIG_DEC(), Types.STRING(),
    Types.BIG_DEC(), Types.STRING(), Types.LONG(),
])
INSERT_SQL = f"INSERT INTO ad_revenue_events ({', '.join(COLUMNS)}) VALUES ({', '.join('?' * len(COLUMNS))})"


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


class DebeziumToAdRevenueRow(MapFunction):
    def map(self, raw: str) -> Row:
        payload = json.loads(raw).get("payload", {})
        after = payload.get("after") or payload.get("before") or {}
        values = {
            "id": after.get("id"),
            "created_at": _to_timestamp(after.get("created_at")),
            "store_id": after.get("store_id"),
            "app_version": after.get("app_version"),
            "tracker_name": after.get("tracker_name"),
            "network_name": after.get("network_name"),
            "campaign_name": after.get("campaign_name"),
            "adgroup_name": after.get("adgroup_name"),
            "creative_name": after.get("creative_name"),
            "country": after.get("country"),
            "adid": after.get("adid"),
            "ad_revenue_network": after.get("ad_revenue_network"),
            "ad_revenue_placement": after.get("ad_revenue_placement"),
            "ad_revenue_unit": after.get("ad_revenue_unit"),
            "reporting_revenue": _numeric(after.get("reporting_revenue")),
            "reporting_currency": after.get("reporting_currency"),
            "ad_impression_count": _numeric(after.get("ad_impression_count")),
            "op": payload.get("op"),
            "__debezium_ts_ms": payload.get("ts_ms"),
        }
        return Row(*(values[c] for c in COLUMNS))


def main():
    config = Configuration()
    config.set_string("execution.checkpointing.interval", "1min")
    config.set_string("state.checkpoints.dir", "file:///opt/flink/checkpoints/sink_to_clickhouse/")

    env = StreamExecutionEnvironment.get_execution_environment(config)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(AD_REVENUE_TOPIC)
        .set_group_id("flink-clickhouse-ad-revenue")
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

    env.from_source(source, WatermarkStrategy.no_watermarks(), "ad-revenue-source").map(
        DebeziumToAdRevenueRow(), output_type=ROW_TYPE
    ).add_sink(JdbcSink.sink(INSERT_SQL, ROW_TYPE, connection_options, execution_options))

    env.execute("sink-ad-revenue-to-clickhouse")


if __name__ == "__main__":
    main()
