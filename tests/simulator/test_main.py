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
    assert attrs["app_version"] in main.APP_VERSIONS
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
    assert params["adid"] == "adid_abc123"
    assert params["store_id"] == attrs["store_id"]


def test_insert_ad_revenue():
    cur = MagicMock()
    cur.fetchone.return_value = (7, "adid_abc123", 0.05, "USD")
    attrs = main.random_tracker_attrs()

    result = main.insert_ad_revenue(cur, attrs, "adid_abc123")

    assert result == {"id": 7, "adid": "adid_abc123", "reporting_revenue": 0.05, "reporting_currency": "USD"}
    cur.execute.assert_called_once()
    params = cur.execute.call_args[0][1]
    assert params["ad_revenue_network"] in main.AD_REVENUE_NETWORKS
    assert params["ad_revenue_placement"] in main.AD_REVENUE_PLACEMENTS
    assert params["ad_revenue_unit"] in main.AD_REVENUE_UNITS
    assert params["currency"] in main.CURRENCIES
    assert 0.0001 <= params["revenue"] <= 0.1
    assert 1 <= params["impressions"] <= 50


@patch("main.random.randint", return_value=3)
@patch("main.get_connection")
def test_fire_random_install_event(mock_get_connection, _mock_randint):
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    mock_get_connection.return_value = conn

    cur.fetchone.side_effect = [
        (1,),  # insert_install
        (10, "adid", 0.01, "USD"),
        (11, "adid", 0.02, "USD"),
        (12, "adid", 0.03, "USD"),
    ]

    result = main.fire_random_install_event()

    assert result["install"]["id"] == 1
    assert len(result["ad_revenue_events"]) == 3
    conn.close.assert_called_once()
