CREATE SCHEMA IF NOT EXISTS adjust;

CREATE TABLE adjust.event__install (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    store_id VARCHAR(255),
    app_version VARCHAR(255),
    tracker_name VARCHAR(255),
    network_name VARCHAR(255),
    campaign_name VARCHAR(255),
    adgroup_name VARCHAR(255),
    creative_name VARCHAR(255),
    country VARCHAR(255),
    adid VARCHAR(255)
);

ALTER TABLE adjust.event__install REPLICA IDENTITY FULL;

CREATE TABLE adjust.event__ad_revenue (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    store_id VARCHAR(255),
    app_version VARCHAR(255),
    tracker_name VARCHAR(255),
    network_name VARCHAR(255),
    campaign_name VARCHAR(255),
    adgroup_name VARCHAR(255),
    creative_name VARCHAR(255),
    country VARCHAR(255),
    adid VARCHAR(255),
    ad_revenue_network VARCHAR(255),
    ad_revenue_placement VARCHAR(255),
    ad_revenue_unit VARCHAR(255),
    reporting_revenue NUMERIC(18, 6),
    reporting_currency VARCHAR(10),
    ad_impression_count NUMERIC(18, 6)
);

ALTER TABLE adjust.event__ad_revenue REPLICA IDENTITY FULL;

CREATE TABLE adjust.event__subscription (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    store_id VARCHAR(255),
    tracker_name VARCHAR(255),
    network_name VARCHAR(255),
    campaign_name VARCHAR(255),
    adgroup_name VARCHAR(255),
    creative_name VARCHAR(255),
    country VARCHAR(255),
    adid VARCHAR(255),
    product_id VARCHAR(255),
    reporting_revenue NUMERIC(18, 6),
    reporting_currency VARCHAR(10)
);

ALTER TABLE adjust.event__subscription REPLICA IDENTITY FULL;
