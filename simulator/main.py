import asyncio
import os
import random
import string
from contextlib import asynccontextmanager

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
COUNTRIES = ["VN", "US", "SG", "JP", "TH"]
APP_VERSIONS = ["1.0.0", "1.1.0", "1.2.3", "2.0.0"]
CURRENCIES = ["USD"]
AD_REVENUE_NETWORKS = ["AdMob", "Facebook Audience Network", "AppLovin", "Unity Ads"]
AD_REVENUE_PLACEMENTS = ["banner", "interstitial", "rewarded", "native"]
AD_REVENUE_UNITS = ["unit_a", "unit_b", "unit_c"]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def random_id_string(prefix: str, length: int = 8) -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=length))
    return f"{prefix}_{suffix}"


def random_tracker_attrs():
    return {
        "store_id": random.choice(STORES),
        "app_version": random.choice(APP_VERSIONS),
        "tracker_name": random_id_string("trk"),
        "network_name": random.choice(NETWORKS),
        "campaign_name": random.choice(CAMPAIGNS),
        "adgroup_name": random.choice(ADGROUPS),
        "creative_name": random.choice(CREATIVES),
        "country": random.choice(COUNTRIES),
    }


def insert_install(cur, attrs, adid):
    cur.execute(
        """
        INSERT INTO adjust.event__install
            (created_at, store_id, app_version, tracker_name, network_name, campaign_name, adgroup_name, creative_name, country, adid)
        VALUES
            (NOW(), %(store_id)s, %(app_version)s, %(tracker_name)s, %(network_name)s, %(campaign_name)s, %(adgroup_name)s, %(creative_name)s, %(country)s, %(adid)s)
        RETURNING id
        """,
        {**attrs, "adid": adid},
    )
    return cur.fetchone()[0]


def insert_ad_revenue(cur, attrs, adid):
    revenue = round(random.uniform(0.0001, 0.1), 6)
    currency = random.choice(CURRENCIES)
    impressions = round(random.uniform(1, 50), 6)
    cur.execute(
        """
        INSERT INTO adjust.event__ad_revenue
            (created_at, store_id, app_version, tracker_name, network_name, campaign_name, adgroup_name, creative_name, country, adid,
             ad_revenue_network, ad_revenue_placement, ad_revenue_unit, reporting_revenue, reporting_currency, ad_impression_count)
        VALUES
            (NOW(), %(store_id)s, %(app_version)s, %(tracker_name)s, %(network_name)s, %(campaign_name)s, %(adgroup_name)s, %(creative_name)s, %(country)s, %(adid)s,
             %(ad_revenue_network)s, %(ad_revenue_placement)s, %(ad_revenue_unit)s, %(revenue)s, %(currency)s, %(impressions)s)
        RETURNING id, adid, reporting_revenue, reporting_currency
        """,
        {
            **attrs,
            "adid": adid,
            "ad_revenue_network": random.choice(AD_REVENUE_NETWORKS),
            "ad_revenue_placement": random.choice(AD_REVENUE_PLACEMENTS),
            "ad_revenue_unit": random.choice(AD_REVENUE_UNITS),
            "revenue": revenue,
            "currency": currency,
            "impressions": impressions,
        },
    )
    row = cur.fetchone()
    return {"id": row[0], "adid": row[1], "reporting_revenue": float(row[2]), "reporting_currency": row[3]}


def fire_random_install_event():
    attrs = random_tracker_attrs()
    adid = random_id_string("adid")
    ad_revenue_count = random.randint(0, 10)

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                install_id = insert_install(cur, attrs, adid)
                ad_revenue_events = [insert_ad_revenue(cur, attrs, adid) for _ in range(ad_revenue_count)]
    finally:
        conn.close()

    return {
        "install": {"id": install_id, "adid": adid, **attrs},
        "ad_revenue_events": ad_revenue_events,
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
        await asyncio.sleep(random.uniform(0.01, 60))
