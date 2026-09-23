import importlib
import os

import yaml
from pyspark.sql import SparkSession

CONFIG_JOBS = [
    'bronze.sink_adjust_event_to_bronze'
]


def create_spark_session(app_name: str, catalog_name: str = "iceberg") -> SparkSession:
    # Lấy thông tin cấu hình từ biến môi trường (hoặc dùng giá trị mặc định)
    minio_endpoint = os.getenv("S3_ENDPOINT")
    rest_uri = os.getenv("ICEBERG_REST_URI")
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
        .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog_name}.type", "rest") \
        .config(f"spark.sql.catalog.{catalog_name}.uri", rest_uri) \
        .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", minio_endpoint) \
        .config(f"spark.sql.catalog.{catalog_name}.s3.path-style-access", "true") \
        .config(f"spark.sql.catalog.{catalog_name}.s3.access-key-id", aws_access_key) \
        .config(f"spark.sql.catalog.{catalog_name}.s3.secret-access-key", aws_secret_key) \
        .config(f"spark.sql.catalog.{catalog_name}.client.region", aws_region) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
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

    spark = create_spark_session("StreamingJobs", iceberg_catalog)

    for job_key in CONFIG_JOBS:
        layer, job = job_key.split(".", 1)
        config = load_job_config(layer, job)
        module = importlib.import_module(f"{layer}.{config['source']}")
        config["kafka_bootstrap_servers"] = kafka_bootstrap_servers
        config["iceberg_catalog"] = iceberg_catalog
        config["iceberg_table"] = f"{iceberg_catalog}.{layer}.{config['table']}"
        config["checkpoint_location"] = os.getenv(
            "CHECKPOINT_LOCATION", f"s3a://data/checkpoints/{env}/{layer}/{job}"
        )
        module.run(spark, config)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
