def _ensure_table(spark, table):
    if spark.catalog.tableExists(table):
        return
    spark.sql(f"""
        CREATE TABLE {table} (
            key STRING,
            value STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            timestamp TIMESTAMP,
            event_date STRING
        )
        USING iceberg
        PARTITIONED BY (event_date)
    """)


def run(spark, config):
    spark.conf.set("spark.sql.shuffle.partitions", str(config["shuffle_partitions"]))
    _ensure_table(spark, config["iceberg_table"])

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", config["kafka_bootstrap_servers"]) \
        .option("subscribe", config["kafka_topic"]) \
        .option("kafka.group.id", config["kafka_group_id"]) \
        .load() \
        .selectExpr(
            "CAST(key AS STRING) as key",
            "CAST(value AS STRING) as value",
            "partition as kafka_partition",
            "offset as kafka_offset",
            "timestamp",
            "date_format(timestamp, 'yyyy-MM-dd') as event_date",
        )

    return df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .option("checkpointLocation", config["checkpoint_location"]) \
        .trigger(processingTime=config["trigger_interval"]) \
        .toTable(config["iceberg_table"])
