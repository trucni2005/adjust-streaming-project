# ---- simulator ----
FROM python:3.11-slim AS simulator

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY simulator/main.py .

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# ---- spark ----
FROM apache/spark:4.0.0-scala2.13-java17-python3-ubuntu AS spark

USER root

RUN pip install pyyaml requests

COPY jars.txt /tmp/jars.txt

RUN cd /opt/spark/jars && \
    grep -v '^$' /tmp/jars.txt | while read jar; do \
        org=$(echo "$jar" | cut -d: -f1); \
        artifact=$(echo "$jar" | cut -d: -f2); \
        version=$(echo "$jar" | cut -d: -f3); \
        org_path=$(echo "$org" | sed 's/\./\//g'); \
        url="https://repo1.maven.org/maven2/${org_path}/${artifact}/${version}/${artifact}-${version}.jar"; \
        echo "Downloading: $url"; \
        curl -fL -O "$url" || exit 1; \
    done

COPY spark /app
