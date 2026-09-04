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

CREATE TABLE IF NOT EXISTS default.spark__install_events
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
