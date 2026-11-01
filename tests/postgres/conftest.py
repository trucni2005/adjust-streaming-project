"""Integration test cho container postgres (compose/postgres.yml).

Chạy với DB thật qua port publish trên host (127.0.0.1:5434), dùng đúng các user
mà simulator/Debezium dùng. Không kết nối được thì skip toàn bộ, kèm lý do.

    make postgres-up && make test-postgres
"""
import os
from pathlib import Path

import psycopg2
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def load_dotenv(path):
    # Không thêm dependency python-dotenv: .env chỉ có dạng KEY=VALUE.
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


load_dotenv(REPO_ROOT / ".env")


def connect_params(user, password):
    return {
        # PG_HOST trong .env là tên container (postgres), không resolve được từ host.
        "host": os.getenv("PG_TEST_HOST", "127.0.0.1"),
        "port": os.getenv("PG_TEST_PORT", "5434"),
        "dbname": os.getenv("PG_DATABASE", "postgres"),
        "user": user,
        "password": password,
        "connect_timeout": 3,
    }


def superuser_password():
    secret = REPO_ROOT / "secrets" / "pg_superuser_password"
    return secret.read_text().strip() if secret.exists() else None


@pytest.fixture(scope="session")
def superuser_params():
    password = superuser_password()
    if password is None:
        pytest.skip("Thiếu secrets/pg_superuser_password (chạy make postgres-up)")
    params = connect_params(os.getenv("PG_SUPERUSER", "postgres"), password)
    try:
        psycopg2.connect(**params).close()
    except psycopg2.OperationalError as exc:
        # DB đang chạy nhưng sai password là lỗi cấu hình thật, không được skip.
        if "password authentication failed" in str(exc):
            pytest.fail(
                "Password superuser trong DB không khớp secrets/pg_superuser_password "
                f"(chạy make postgres-roles để đồng bộ): {exc}"
            )
        pytest.skip(f"Không kết nối được postgres tại {params['host']}:{params['port']}: {exc}")
    return params


def env_credentials(user_var, password_var):
    user, password = os.getenv(user_var), os.getenv(password_var)
    if not user or not password:
        pytest.skip(f"Thiếu {user_var}/{password_var} trong .env")
    return connect_params(user, password)


@pytest.fixture(scope="session")
def app_params(superuser_params):
    return env_credentials("PG_USER", "PG_PASSWORD")


@pytest.fixture(scope="session")
def debezium_params(superuser_params):
    return env_credentials("PG_DEBEZIUM_USER", "PG_DEBEZIUM_PASSWORD")


def open_cursor(params):
    # Mọi thay đổi trong test đều rollback khi đóng, không để lại dữ liệu.
    conn = psycopg2.connect(**params)
    try:
        with conn.cursor() as cur:
            yield cur
    finally:
        conn.rollback()
        conn.close()


@pytest.fixture
def superuser_cur(superuser_params):
    yield from open_cursor(superuser_params)


@pytest.fixture
def app_cur(app_params):
    yield from open_cursor(app_params)


@pytest.fixture
def debezium_cur(debezium_params):
    yield from open_cursor(debezium_params)
