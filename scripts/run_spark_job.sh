#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

# Installed to a fixed path (not $HOME/.local) because executors run with
# HOME=/nonexistent, unwritable, unlike the driver container.
for container in spark-master adjust-streaming-project-spark-worker-1-1 adjust-streaming-project-spark-worker-2-1; do
  docker exec "$container" pip install --quiet --target=/tmp/spark-libs kafka-python
done

docker exec spark-master mkdir -p /data/spark-events

docker exec -e PYTHONPATH=/tmp/spark-libs spark-master sh -c '
  exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages "$KAFKA_CONNECTOR_PACKAGE,io.delta:delta-spark_2.12:3.2.0" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.executorEnv.PYTHONPATH=/tmp/spark-libs \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=/data/spark-events \
    /app/bronze/sink_to_bronze.py
'
