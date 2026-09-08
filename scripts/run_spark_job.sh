#!/usr/bin/env bash
set -euo pipefail
export MSYS_NO_PATHCONV=1

# Installed to a fixed path (not $HOME/.local) because executors run with
# HOME=/nonexistent, unwritable, unlike the driver container.
for container in spark-master adjust-streaming-project-spark-worker-1-1 adjust-streaming-project-spark-worker-2-1; do
  docker exec "$container" pip install --quiet --target=/tmp/spark-libs kafka-python
done

docker exec -e PYTHONPATH=/tmp/spark-libs spark-master sh -c '
  exec /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages "$KAFKA_CONNECTOR_PACKAGE,org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.0" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.executorEnv.PYTHONPATH=/tmp/spark-libs \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
    /app/sink_to_silver.py
'
