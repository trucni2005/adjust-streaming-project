import importlib
import os

import yaml
from pyspark.sql import SparkSession

CONFIG_JOBS = [
    'bronze.sink_adjust_event_to_bronze'
]


def create_spark_session(app_name: str, iceberg_catalog: str, iceberg_warehouse: str) -> SparkSession:
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{iceberg_catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{iceberg_catalog}.type", "hadoop") \
        .config(f"spark.sql.catalog.{iceberg_catalog}.warehouse", iceberg_warehouse) \
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def load_job_config(layer: str, job: str) -> dict:
    config_path = os.path.join(os.path.dirname(__file__), layer, "config.yaml")
    with open(config_path) as f:
        all_config = yaml.safe_load(f)
    return all_config[job]


def main():
    env = os.getenv("ENV", "dev")
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092,kafka2:9092,kafka3:9092")
    iceberg_catalog = os.getenv("ICEBERG_CATALOG", "catalog")
    iceberg_warehouse = os.getenv("ICEBERG_WAREHOUSE", f"s3a://lakehouse/{env}")

    spark = create_spark_session("StreamingJobs", iceberg_catalog, iceberg_warehouse)

    for job_key in CONFIG_JOBS:
        layer, job = job_key.split(".", 1)
        config = load_job_config(layer, job)
        module = importlib.import_module(f"{layer}.{config['source']}")
        config["kafka_bootstrap_servers"] = kafka_bootstrap_servers
        config["iceberg_catalog"] = iceberg_catalog
        config["iceberg_warehouse"] = iceberg_warehouse
        config["iceberg_table"] = f"{iceberg_catalog}.{layer}.{config['table']}"
        config["checkpoint_location"] = os.getenv(
            "CHECKPOINT_LOCATION", f"s3a://data/checkpoints/{env}/{layer}/{job}"
        )
        module.run(spark, config)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
