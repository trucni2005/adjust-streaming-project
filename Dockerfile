# ---- simulator ----
FROM python:3.11-slim AS simulator

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator/main.py .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# ---- spark ----
FROM apache/spark:4.0.0-scala2.13-java17-python3-ubuntu AS spark

USER root

RUN mkdir -p /conf/jars && \
    cd /conf/jars && \
    curl -fL -o iceberg-spark-runtime-4.0_2.13-1.11.0.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.11.0/iceberg-spark-runtime-4.0_2.13-1.11.0.jar