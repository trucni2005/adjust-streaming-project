import os

from pyspark.sql import SparkSession

ICEBERG_CATALOG = "bronze_catalog"
ICEBERG_TABLE = f"{ICEBERG_CATALOG}.default.bronze_events"
ICEBERG_WAREHOUSE = os.getenv("BRONZE_ICEBERG_WAREHOUSE", "/lakehouse")
CHECKPOINT_LOCATION = os.getenv("BRONZE_CHECKPOINT_LOCATION", "/data/checkpoints/sink_to_bronze")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "adjust-dbserver.adjust.event")

def _ensure_table(spark):
    if spark.catalog.tableExists(ICEBERG_TABLE):
        return
    spark.sql(f"""
        CREATE TABLE {ICEBERG_TABLE} (value STRING, event_date STRING)
        USING iceberg
        PARTITIONED BY (event_date)
    """)


def run(spark):
    _ensure_table(spark)

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("kafka.group.id", "spark-streaming-consumer") \
        .load() \
        .selectExpr(
            "CAST(value AS STRING) as value",
            "date_format(timestamp, 'yyyy-MM-dd') as event_date",
        )

    query = df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="1 minutes") \
        .toTable(ICEBERG_TABLE)

    query.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SinkToBronze") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.extraClassPath", "/conf/jars/*") \
        .config("spark.executor.extraClassPath", "/conf/jars/*") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE) \
        .getOrCreate()
    run(spark)
    spark.stop()
