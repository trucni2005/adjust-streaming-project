# ---- simulator ----
FROM python:3.11-slim AS simulator

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator/main.py .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

# ---- pyflink ----
FROM flink:1.19.1-scala_2.12-java11 AS pyflink

RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev && \
    rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip3 install apache-flink==1.19.1

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.2.0-1.19/flink-sql-connector-kafka-3.2.0-1.19.jar

# Parquet bulk writer support (row -> Avro -> Parquet)
# flink-sql-avro is the shaded jar (bundles Avro core under org.apache.flink.avro.shaded.*),
# required because plain flink-avro does not bundle Avro itself and AvroSchema.parse_string()
# needs the shaded Schema.Parser class available at Python client submission time.
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.19.1/flink-parquet-1.19.1.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro/1.19.1/flink-sql-avro-1.19.1.jar

# JDBC sink (used by sink_to_clickhouse.py) + ClickHouse's driver, shaded jar bundles its own deps.
# Pinned to the 3.1.x line: 3.2.0+ dropped JdbcOutputFormat.createRowJdbcStatementBuilder(),
# which PyFlink 1.19.1's JdbcSink.sink() Python wrapper still reflects on directly.
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.10.0/clickhouse-jdbc-0.10.0-all.jar

COPY flink/jobs/ /opt/flink/usrlib/jobs/