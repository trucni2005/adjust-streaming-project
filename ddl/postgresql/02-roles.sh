#!/bin/bash
# Tạo user theo quyền tối thiểu cho simulator và Debezium.
# Idempotent: tự chạy lúc initdb (docker-entrypoint-initdb.d), chạy lại trên DB
# đang có bằng `make postgres-roles` để đồng bộ password/quyền theo .env.
set -euo pipefail

psql -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  -v su_user="$POSTGRES_USER" \
  -v su_password="$(cat "$POSTGRES_PASSWORD_FILE")" \
  -v app_user="$PG_APP_USER" \
  -v app_password="$PG_APP_PASSWORD" \
  -v dbz_user="$PG_DEBEZIUM_USER" \
  -v dbz_password="$PG_DEBEZIUM_PASSWORD" \
  <<'EOSQL'
-- POSTGRES_PASSWORD_FILE chỉ áp dụng lúc initdb; set lại để đổi secret có hiệu lực.
ALTER ROLE :"su_user" PASSWORD :'su_password';

-- simulator: chỉ INSERT. SELECT(id) cần cho INSERT ... RETURNING id.
SELECT format('CREATE ROLE %I LOGIN', :'app_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'app_user') \gexec
ALTER ROLE :"app_user" PASSWORD :'app_password';
GRANT CONNECT ON DATABASE :"DBNAME" TO :"app_user";
GRANT USAGE ON SCHEMA adjust TO :"app_user";
GRANT INSERT ON adjust.event TO :"app_user";
GRANT SELECT (id) ON adjust.event TO :"app_user";
GRANT USAGE ON SEQUENCE adjust.event_id_seq TO :"app_user";

-- debezium: REPLICATION để đọc WAL, SELECT để snapshot.
SELECT format('CREATE ROLE %I LOGIN REPLICATION', :'dbz_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'dbz_user') \gexec
ALTER ROLE :"dbz_user" PASSWORD :'dbz_password';
GRANT CONNECT ON DATABASE :"DBNAME" TO :"dbz_user";
GRANT USAGE ON SCHEMA adjust TO :"dbz_user";
GRANT SELECT ON adjust.event TO :"dbz_user";

-- User debezium không phải superuser nên không tự tạo publication được
-- (connector chạy với publication.autocreate.mode=disabled).
SELECT 'CREATE PUBLICATION dbz_publication FOR TABLE adjust.event'
WHERE NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dbz_publication') \gexec
EOSQL
