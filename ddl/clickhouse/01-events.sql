-- Target tables for pyspark/sink_to_clickhouse.py.
--
-- ReplacingMergeTree is used (keyed on the Postgres id) because Debezium can
-- redeliver the same change event (at-least-once delivery) and ClickHouse's
-- plain MergeTree does not dedupe on insert. ReplacingMergeTree collapses
-- duplicate ids down to the row with the highest version (__debezium_ts_ms)
-- during background merges / when queried with FINAL. This is an eventual
-- dedup, not immediate — add FINAL to a query or run OPTIMIZE TABLE ...
-- FINAL if guaranteed-latest-only rows are needed right after insert.
--
-- Not wired into docker-compose.yml's clickhouse service volumes yet — apply
-- manually via clickhouse-client until/unless
-- ./clickhouse/init:/docker-entrypoint-initdb.d is added there.

CREATE TABLE IF NOT EXISTS default.flink__install_events
(
    id                  Int64,
    created_at          DateTime64(3),
    store_id            String,
    app_version         String,
    tracker_name        String,
    network_name        String,
    campaign_name       String,
    adgroup_name        String,
    creative_name       String,
    country             String,
    adid                String,
    op                  LowCardinality(String),
    __debezium_ts_ms    Int64
)
ENGINE = ReplacingMergeTree(__debezium_ts_ms)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS default.flink__ad_revenue_events
(
    id                      Int64,
    created_at              DateTime64(3),
    store_id                String,
    app_version             String,
    tracker_name            String,
    network_name            String,
    campaign_name           String,
    adgroup_name            String,
    creative_name           String,
    country                 String,
    adid                    String,
    ad_revenue_network      String,
    ad_revenue_placement    String,
    ad_revenue_unit         String,
    reporting_revenue       Decimal(18, 6),
    reporting_currency      String,
    ad_impression_count     Decimal(18, 6),
    op                      LowCardinality(String),
    __debezium_ts_ms        Int64
)
ENGINE = ReplacingMergeTree(__debezium_ts_ms)
ORDER BY (id);

CREATE TABLE IF NOT EXISTS default.spark__ad_revenue_events
(
    id                      Int64,
    created_at              DateTime64(3),
    store_id                String,
    app_version             String,
    tracker_name            String,
    network_name            String,
    campaign_name           String,
    adgroup_name            String,
    creative_name           String,
    country                 String,
    adid                    String,
    ad_revenue_network      String,
    ad_revenue_placement    String,
    ad_revenue_unit         String,
    reporting_revenue       Decimal(18, 6),
    reporting_currency      String,
    ad_impression_count     Decimal(18, 6),
    op                      LowCardinality(String),
    __debezium_ts_ms        Int64
)
ENGINE = ReplacingMergeTree(__debezium_ts_ms)
ORDER BY (id);
