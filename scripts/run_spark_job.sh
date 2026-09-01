#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

docker exec spark-master pip install --quiet clickhouse-connect pandas kafka-python

docker exec spark-master sh -c '
  exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages "$KAFKA_CONNECTOR_PACKAGE" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /app/sink_to_clickhouse.py
'
