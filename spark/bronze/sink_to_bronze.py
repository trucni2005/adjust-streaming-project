import os
import json
import requests
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.avro.functions import from_avro


def _fetch_schema_from_registry(schema_registry_url: str, subject: str) -> str:
    url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    schema_str = response.json()["schema"]
    return schema_str


def run(spark, config):
    spark.conf.set("spark.sql.shuffle.partitions", str(config["shuffle_partitions"]))

    table_name = config["iceberg_table"]
    if spark.catalog.tableExists(table_name):
        spark.sql(f"DROP TABLE {table_name}")

    schema_file = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_file) as f:
        schema_sql = f.read().replace("catalog", config["iceberg_catalog"])
    spark.sql(schema_sql)

    _input = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"]) \
        .option("subscribe", config["kafka_topic"]) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    schema_registry_url = config.get("schema_registry_url", "http://schema-registry:8081")
    subject = config["kafka_topic"] + "-value"

    avro_schema_str = _fetch_schema_from_registry(schema_registry_url, subject)

    df = _input.select(
        from_avro(col("value"), avro_schema_str, {"mode": "PERMISSIVE"}).alias("data")
    ).select("data.*") \
        .withColumn("event_date", from_unixtime(col("created_at") / 1000000, "yyyy-MM-dd").cast("date"))

    return df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", config["checkpoint_location"]) \
        .trigger(processingTime=config["trigger_interval"]) \
        .toTable(config["iceberg_table"])
