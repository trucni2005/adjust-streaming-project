"""Điều kiện để Debezium chạy với publication.autocreate.mode=disabled."""
import pytest

pytestmark = pytest.mark.integration


def test_publication_covers_only_event_table(superuser_cur):
    superuser_cur.execute(
        "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = 'dbz_publication'"
    )
    assert superuser_cur.fetchall() == [("adjust", "event")]


def test_publication_publishes_all_dml(superuser_cur):
    superuser_cur.execute(
        "SELECT pubinsert, pubupdate, pubdelete, pubtruncate "
        "FROM pg_publication WHERE pubname = 'dbz_publication'"
    )
    assert superuser_cur.fetchone() == (True, True, True, True)


def test_event_table_replica_identity_full(superuser_cur):
    # FULL: event UPDATE/DELETE mang đủ giá trị cũ của mọi cột.
    superuser_cur.execute("SELECT relreplident FROM pg_class WHERE oid = 'adjust.event'::regclass")
    assert superuser_cur.fetchone()[0] == "f"


def test_replication_slot_not_lost(superuser_cur):
    # Slot bị invalidate (vượt max_slot_wal_keep_size) thì connector phải snapshot lại.
    superuser_cur.execute("SELECT slot_name, wal_status FROM pg_replication_slots WHERE slot_type = 'logical'")
    slots = superuser_cur.fetchall()
    if not slots:
        pytest.skip("Chưa có replication slot (chưa chạy make register-connector)")
    assert all(status != "lost" for _, status in slots), slots
