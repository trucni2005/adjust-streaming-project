from unittest.mock import MagicMock, patch

import main


def test_random_id_string_prefix_and_length():
    value = main.random_id_string("adid", length=8)
    prefix, _, suffix = value.partition("_")
    assert prefix == "adid"
    assert len(suffix) == 8
    assert all(c in "abcdefghijklmnopqrstuvwxyz0123456789" for c in suffix)


def test_random_tracker_attrs_keys():
    attrs = main.random_tracker_attrs()
    assert attrs["store_id"] in main.STORES
    assert attrs["app_name"] == main.APP_NAMES[attrs["store_id"]]
    assert attrs["app_version"] in main.APP_VERSIONS
    assert attrs["platform"] in main.PLATFORMS
    assert attrs["os_name"] == attrs["platform"]

    
    assert attrs["os_version"] in main.OS_VERSIONS_BY_PLATFORM[attrs["platform"]]
    assert attrs["device_model"] in main.DEVICE_MODELS_BY_PLATFORM[attrs["platform"]]
    assert attrs["network_name"] in main.NETWORKS
    assert attrs["campaign_name"] in main.CAMPAIGNS
    assert attrs["adgroup_name"] in main.ADGROUPS
    assert attrs["creative_name"] in main.CREATIVES
    assert attrs["country"] in main.COUNTRIES
    assert attrs["tracker_name"].startswith("trk_")


def test_insert_install():
    cur = MagicMock()
    cur.fetchone.return_value = (42,)
    attrs = main.random_tracker_attrs()

    result = main.insert_install(cur, attrs, "adid_abc123")

    assert result == 42
    cur.execute.assert_called_once()
    params = cur.execute.call_args[0][1]
    assert params["activity_kind"] == "install"
    assert params["adid"] == "adid_abc123"
    assert params["gps_adid"] == "adid_abc123"
    assert params["store_id"] == attrs["store_id"]
    assert params["installed_at"] is not None


def test_insert_ad_revenue():
    cur = MagicMock()
    cur.fetchone.return_value = (7,)
    attrs = main.random_tracker_attrs()

    result = main.insert_ad_revenue(cur, attrs, "adid_abc123")

    assert result["id"] == 7
    assert result["adid"] == "adid_abc123"
    assert 0.0001 <= result["reporting_revenue"] <= 0.1
    assert result["reporting_currency"] in main.CURRENCIES
    cur.execute.assert_called_once()
    params = cur.execute.call_args[0][1]
    assert params["activity_kind"] == "ad_revenue"
    assert params["ad_revenue_network"] in main.AD_REVENUE_NETWORKS
    assert params["ad_revenue_placement"] in main.AD_REVENUE_PLACEMENTS
    assert params["ad_revenue_unit"] in main.AD_REVENUE_UNITS
    assert params["reporting_currency"] in main.CURRENCIES
    assert 0.0001 <= params["reporting_revenue"] <= 0.1
    assert 1 <= params["ad_impressions_count"] <= 50


def test_insert_subscription():
    cur = MagicMock()
    cur.fetchone.return_value = (9,)
    attrs = main.random_tracker_attrs()

    result = main.insert_subscription(cur, attrs, "adid_abc123")

    assert result["id"] == 9
    assert result["adid"] == "adid_abc123"
    cur.execute.assert_called_once()
    params = cur.execute.call_args[0][1]
    assert params["activity_kind"] == "subscription"
    assert params["subscription_event_type"] in main.SUBSCRIPTION_EVENT_TYPES
    assert params["subscription_product_id"] in main.SUBSCRIPTION_PRODUCT_IDS
    assert params["subscription_sales_region"] in main.SUBSCRIPTION_SALES_REGIONS
    assert params["subscription_transaction_id"].startswith("txn_")
    assert params["subscription_original_transaction_id"].startswith("otxn_")
    assert params["reporting_currency"] in main.CURRENCIES


@patch("main.random.randint")
@patch("main.get_connection")
def test_fire_random_install_event(mock_get_connection, mock_randint):
    mock_randint.side_effect = [3, 2]  # ad_revenue_count, subscription_count

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    mock_get_connection.return_value = conn

    cur.fetchone.side_effect = [(1,), (10,), (11,), (12,), (20,), (21,)]

    result = main.fire_random_install_event()

    assert result["install"]["id"] == 1
    assert len(result["ad_revenue_events"]) == 3
    assert len(result["subscription_events"]) == 2
    conn.close.assert_called_once()
