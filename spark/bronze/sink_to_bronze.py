import os

from pyspark.sql import SparkSession

ENV = os.getenv("ENV", "dev")

ICEBERG_CATALOG = "bronze_catalog"
ICEBERG_TABLE = f"{ICEBERG_CATALOG}.{ENV}.bronze_events"
ICEBERG_WAREHOUSE = os.getenv("BRONZE_ICEBERG_WAREHOUSE", "s3a://lakehouse")
CHECKPOINT_LOCATION = os.getenv("BRONZE_CHECKPOINT_LOCATION", f"s3a://data/checkpoints/{ENV}/sink_to_bronze")

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
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop") \
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    run(spark)
    spark.stop()
