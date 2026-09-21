import requests
from pyspark.sql.functions import col, expr, from_unixtime
from pyspark.sql.avro.functions import from_avro


def _fetch_schema_from_registry(schema_registry_url: str, subject: str) -> str:
    url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    response.raise_for_status()
    schema_str = response.json()["schema"]
    return schema_str


def run(spark, config):
    spark.conf.set("spark.sql.shuffle.partitions", str(config["shuffle_partitions"]))

    _input = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"]) \
        .option("subscribe", config["kafka_topic"]) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "true") \
        .load()

    schema_registry_url = config.get("schema_registry_url", "http://schema-registry:8081")
    subject = config["kafka_topic"] + "-value"

    avro_schema_str = _fetch_schema_from_registry(schema_registry_url, subject)

    df = _input.select(
        from_avro(expr("substring(value, 6, length(value) - 5)"), avro_schema_str).alias("data")
    ).select("data.*") \
        .withColumn("event_date", from_unixtime(col("created_at") / 1000000, "yyyy-MM-dd").cast("date"))

    return df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", config["checkpoint_location"]) \
        .partitionBy("event_date") \
        .trigger(processingTime=config["trigger_interval"]) \
        .toTable(config["iceberg_table"])
