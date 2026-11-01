"""Quyền tối thiểu của user simulator/Debezium (ddl/postgresql/02-roles.sh)."""
import psycopg2
import pytest
from psycopg2 import errors

import main

pytestmark = pytest.mark.integration


def role_attrs(cur, rolname):
    cur.execute(
        "SELECT rolsuper, rolreplication, rolcreaterole, rolcreatedb, rolbypassrls "
        "FROM pg_roles WHERE rolname = %s",
        (rolname,),
    )
    row = cur.fetchone()
    assert row is not None, f"role {rolname} chưa được tạo"
    return dict(zip(["super", "replication", "createrole", "createdb", "bypassrls"], row))


def assert_denied(cur, sql, params=None):
    with pytest.raises(errors.InsufficientPrivilege):
        cur.execute(sql, params)
    cur.connection.rollback()


# --- simulator_app -----------------------------------------------------------


def test_app_role_has_no_elevated_attributes(superuser_cur, app_params):
    assert not any(role_attrs(superuser_cur, app_params["user"]).values())


def test_app_can_insert_events_via_simulator(app_cur):
    # Dùng đúng code của simulator (INSERT ... RETURNING id) với DB thật.
    event_id = main.insert_install(app_cur, main.random_tracker_attrs(), "adid_pytest")
    assert isinstance(event_id, int)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT adid FROM adjust.event LIMIT 1",
        "UPDATE adjust.event SET adid = 'x' WHERE id = -1",
        "DELETE FROM adjust.event WHERE id = -1",
        "TRUNCATE adjust.event",
        "CREATE TABLE adjust.pytest_tmp (id int)",
        "CREATE TABLE public.pytest_tmp (id int)",
    ],
)
def test_app_denied(app_cur, sql):
    assert_denied(app_cur, sql)


def test_app_cannot_drop_table(app_cur):
    # DROP yêu cầu là owner, lỗi trả về là InsufficientPrivilege (must be owner).
    assert_denied(app_cur, "DROP TABLE adjust.event")


# --- debezium ----------------------------------------------------------------


def test_debezium_role_is_replication_only(superuser_cur, debezium_params):
    attrs = role_attrs(superuser_cur, debezium_params["user"])
    assert attrs.pop("replication") is True
    assert not any(attrs.values())


def test_debezium_can_read_events(debezium_cur):
    # Cần cho snapshot ban đầu của connector.
    debezium_cur.execute("SELECT count(*) FROM adjust.event")
    assert debezium_cur.fetchone()[0] >= 0


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO adjust.event (activity_kind) VALUES ('pytest')",
        "UPDATE adjust.event SET adid = 'x' WHERE id = -1",
        "DELETE FROM adjust.event WHERE id = -1",
        "CREATE PUBLICATION pytest_pub FOR TABLE adjust.event",
    ],
)
def test_debezium_denied(debezium_cur, sql):
    assert_denied(debezium_cur, sql)


def test_debezium_can_open_replication_connection(debezium_params):
    from psycopg2.extras import LogicalReplicationConnection

    conn = psycopg2.connect(**debezium_params, connection_factory=LogicalReplicationConnection)
    try:
        with conn.cursor() as cur:
            cur.execute("IDENTIFY_SYSTEM")
            assert cur.fetchone() is not None
    finally:
        conn.close()


def test_app_cannot_open_replication_connection(app_params):
    from psycopg2.extras import LogicalReplicationConnection

    with pytest.raises(psycopg2.OperationalError):
        psycopg2.connect(**app_params, connection_factory=LogicalReplicationConnection).close()
