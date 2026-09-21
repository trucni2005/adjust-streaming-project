import asyncio
import os
import random
import string
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import psycopg2
from fastapi import FastAPI


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(event_loop())
    yield
    task.cancel()


app = FastAPI(title="Adjust Event Simulator", lifespan=lifespan)

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5434"),
    "dbname": os.getenv("PG_DATABASE", "postgres"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}

NETWORKS = ["Facebook", "Google Ads", "TikTok", "AppLovin", "Unity Ads"]
CAMPAIGNS = ["campaign_a", "campaign_b", "campaign_c", "campaign_d"]
ADGROUPS = ["adgroup_a", "adgroup_b", "adgroup_c"]
CREATIVES = ["creative_a", "creative_b", "creative_c"]
STORES = ["com.example.example1", "com.example.example2", "com.example.example3"]
APP_NAMES = {"com.example.example1": "Example One", "com.example.example2": "Example Two", "com.example.example3": "Example Three"}
COUNTRIES = ["VN", "US", "SG", "JP", "TH"]
APP_VERSIONS = ["1.0.0", "1.1.0", "1.2.3", "2.0.0"]
CURRENCIES = ["USD"]
AD_REVENUE_NETWORKS = ["AdMob", "Facebook Audience Network", "AppLovin", "Unity Ads"]
AD_REVENUE_PLACEMENTS = ["banner", "interstitial", "rewarded", "native"]
AD_REVENUE_UNITS = ["unit_a", "unit_b", "unit_c"]

PLATFORMS = ["ios", "android"]
ENVIRONMENTS = ["sandbox", "production"]
SDK_VERSIONS = ["4.29.0", "4.33.1", "4.38.0", "5.0.1"]
OS_VERSIONS_BY_PLATFORM = {
    "ios": ["15.0", "16.2", "17.1", "18.0"],
    "android": ["11", "12", "13", "14"],
}
DEVICE_TYPES = ["phone", "tablet"]
DEVICE_MODELS_BY_PLATFORM = {
    "ios": ["iPhone14,2", "iPhone15,3", "iPad13,1"],
    "android": ["Pixel 7", "Pixel 8", "SM-G991B", "SM-S911B"],
}
LANGUAGES = ["en", "vi", "ja", "th", "zh"]
COUNTRY_SUBDIVISIONS = ["01", "02", "03", "04", "05"]
CITIES = ["Ho Chi Minh City", "Hanoi", "Singapore", "Tokyo", "Bangkok", "New York"]
TIMEZONES = ["Asia/Ho_Chi_Minh", "Asia/Singapore", "Asia/Tokyo", "Asia/Bangkok", "America/New_York"]
AD_MEDIATION_PLATFORMS = ["AdMob", "MAX", "LevelPlay", "TopOn"]
SUBSCRIPTION_EVENT_TYPES = ["started", "renewed", "cancelled", "expired"]
SUBSCRIPTION_SALES_REGIONS = ["US", "VN", "SG", "JP", "TH"]
SUBSCRIPTION_PRODUCT_IDS = ["sub_monthly", "sub_yearly", "sub_weekly"]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def random_id_string(prefix: str, length: int = 8) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return f"{prefix}_{suffix}"


def random_timestamp_before(dt, max_days_before=30):
    return dt - timedelta(seconds=random.uniform(0, max_days_before * 86400))


def random_tracker_attrs():
    store_id = random.choice(STORES)
    platform = random.choice(PLATFORMS)
    return {
        "app_token": random_id_string("app", 12),
        "store_id": store_id,
        "app_name": APP_NAMES[store_id],
        "app_version": random.choice(APP_VERSIONS),
        "platform": platform,
        "environment": random.choice(ENVIRONMENTS),
        "sdk_version": random.choice(SDK_VERSIONS),
        "os_name": platform,
        "os_version": random.choice(OS_VERSIONS_BY_PLATFORM[platform]),
        "device_type": random.choice(DEVICE_TYPES),
        "device_model": random.choice(DEVICE_MODELS_BY_PLATFORM[platform]),
        "language": random.choice(LANGUAGES),
        "country": random.choice(COUNTRIES),
        "country_subdivision": random.choice(COUNTRY_SUBDIVISIONS),
        "city": random.choice(CITIES),
        "timezone": random.choice(TIMEZONES),
        "tracker_name": random_id_string("trk"),
        "network_name": random.choice(NETWORKS),
        "campaign_name": random.choice(CAMPAIGNS),
        "adgroup_name": random.choice(ADGROUPS),
        "creative_name": random.choice(CREATIVES),
    }


EVENT_COLUMNS = [
    "activity_kind", "app_token", "store_id", "app_name", "app_version",
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
    "impression_based", "is_organic",
]


def insert_event(cur, activity_kind, attrs, adid, extra=None):
    row = {column: None for column in EVENT_COLUMNS}
    row.update({
        "activity_kind": activity_kind,
        "adid": adid,
        "gps_adid": adid,
        "tracker": attrs["tracker_name"],
        **attrs,
    })
    if extra:
        row.update(extra)

    columns_sql = ", ".join(EVENT_COLUMNS)
    placeholders_sql = ", ".join(f"%({column})s" for column in EVENT_COLUMNS)
    cur.execute(
        f"""
        INSERT INTO adjust.event
            (created_at, {columns_sql})
        VALUES
            (NOW(), {placeholders_sql})
        RETURNING id
        """,
        row,
    )
    return cur.fetchone()[0]


def insert_install(cur, attrs, adid):
    now = datetime.now(timezone.utc)
    return insert_event(cur, "install", attrs, adid, extra={
        "event": "install",
        "event_name": "Install",
        "installed_at": now,
        "click_time": random_timestamp_before(now, max_days_before=7) if random.random() < 0.7 else None,
        "impression_time": random_timestamp_before(now, max_days_before=7) if random.random() < 0.3 else None,
        "engagement_time": now,
        "impression_based": random.random() < 0.1,
        "is_organic": random.random() < 0.2,
        "idfa": random_id_string("idfa") if attrs["platform"] == "ios" else None,
        "idfv": random_id_string("idfv") if attrs["platform"] == "ios" else None,
    })


def insert_ad_revenue(cur, attrs, adid):
    revenue = round(random.uniform(0.0001, 0.1), 6)
    currency = random.choice(CURRENCIES)
    impressions = round(random.uniform(1, 50), 6)
    ad_revenue_network = random.choice(AD_REVENUE_NETWORKS)
    ad_revenue_placement = random.choice(AD_REVENUE_PLACEMENTS)
    ad_revenue_unit = random.choice(AD_REVENUE_UNITS)
    row_id = insert_event(cur, "ad_revenue", attrs, adid, extra={
        "event": "ad_revenue",
        "event_name": "Ad Revenue",
        "revenue_float": revenue,
        "currency": currency,
        "reporting_revenue": revenue,
        "reporting_currency": currency,
        "ad_impressions_count": impressions,
        "ad_mediation_platform": random.choice(AD_MEDIATION_PLATFORMS),
        "ad_revenue_network": ad_revenue_network,
        "ad_revenue_placement": ad_revenue_placement,
        "ad_revenue_unit": ad_revenue_unit,
    })
    return {"id": row_id, "adid": adid, "reporting_revenue": revenue, "reporting_currency": currency}


def insert_subscription(cur, attrs, adid):
    now = datetime.now(timezone.utc)
    purchased_at = random_timestamp_before(now, max_days_before=180)
    revenue = round(random.uniform(0.99, 99.99), 6)
    currency = random.choice(CURRENCIES)
    subscription_event_type = random.choice(SUBSCRIPTION_EVENT_TYPES)
    row_id = insert_event(cur, "subscription", attrs, adid, extra={
        "event": "subscription",
        "event_name": "Subscription",
        "reporting_revenue": revenue,
        "reporting_currency": currency,
        "reporting_cost": round(revenue * random.uniform(0.1, 0.3), 6),
        "subscription_event_type": subscription_event_type,
        "subscription_purchased_at": purchased_at,
        "subscription_expiration_time": purchased_at + timedelta(days=30),
        "subscription_cancelled_at": now if subscription_event_type == "cancelled" else None,
        "subscription_transaction_id": random_id_string("txn", 12),
        "subscription_original_transaction_id": random_id_string("otxn", 12),
        "subscription_product_id": random.choice(SUBSCRIPTION_PRODUCT_IDS),
        "subscription_sales_region": random.choice(SUBSCRIPTION_SALES_REGIONS),
    })
    return {"id": row_id, "adid": adid, "reporting_revenue": revenue, "reporting_currency": currency}


def fire_random_install_event():
    attrs = random_tracker_attrs()
    adid = random_id_string("adid")
    ad_revenue_count = random.randint(0, 50)
    subscription_count = random.randint(0, 3)

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                install_id = insert_install(cur, attrs, adid)
                ad_revenue_events = [insert_ad_revenue(cur, attrs, adid) for _ in range(ad_revenue_count)]
                subscription_events = [insert_subscription(cur, attrs, adid) for _ in range(subscription_count)]
    finally:
        conn.close()

    return {
        "install": {"id": install_id, "adid": adid, **attrs},
        "ad_revenue_events": ad_revenue_events,
        "subscription_events": subscription_events,
    }


@app.post("/events/install")
def create_random_install_event():
    return fire_random_install_event()


@app.get("/health")
def health():
    return {"status": "ok"}


async def event_loop():
    while True:
        try:
            fire_random_install_event()
        except Exception as exc:
            print(f"event_loop error: {exc}")
        await asyncio.sleep(random.uniform(0.1, 1))
