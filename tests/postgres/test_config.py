"""Server chạy đúng config/postgres/postgresql.conf và initdb args."""
import psycopg2
import pytest

pytestmark = pytest.mark.integration


def show(cur, setting):
    cur.execute(f"SHOW {setting}")
    return cur.fetchone()[0]


@pytest.mark.parametrize(
    ("setting", "expected"),
    [
        # Debezium cần logical decoding.
        ("wal_level", "logical"),
        # Chặn replication slot treo làm đầy disk.
        ("max_slot_wal_keep_size", "10GB"),
        ("password_encryption", "scram-sha-256"),
        # Cột adjust.event là TIMESTAMP không tz, giá trị đang lưu theo giờ VN.
        ("TimeZone", "Asia/Ho_Chi_Minh"),
        ("config_file", "/etc/postgresql/postgresql.conf"),
    ],
)
def test_setting(superuser_cur, setting, expected):
    assert show(superuser_cur, setting) == expected


def test_data_checksums_enabled(superuser_cur):
    assert show(superuser_cur, "data_checksums") == "on"


def test_replication_capacity(superuser_cur):
    assert int(show(superuser_cur, "max_wal_senders")) >= 1
    assert int(show(superuser_cur, "max_replication_slots")) >= 1


def test_superuser_default_password_rejected(superuser_params):
    params = {**superuser_params, "password": "postgres"}
    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(**params).close()


def test_passwords_stored_as_scram(superuser_cur, app_params, debezium_params):
    superuser_cur.execute(
        "SELECT rolname, rolpassword FROM pg_authid WHERE rolname = ANY(%s)",
        ([app_params["user"], debezium_params["user"]],),
    )
    rows = dict(superuser_cur.fetchall())
    assert set(rows) == {app_params["user"], debezium_params["user"]}
    assert all(password.startswith("SCRAM-SHA-256$") for password in rows.values())
