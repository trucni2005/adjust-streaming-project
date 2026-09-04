#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

docker compose exec flink-jobmanager flink run \
  -py /opt/flink/usrlib/jobs/sink_to_clickhouse.py