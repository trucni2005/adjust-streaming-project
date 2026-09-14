-include spark-env.sh
-include .env
.PHONY: up down build restart logs ps submit-bronze register-connector connector-status

up:
	mkdir -p kafka-cluster-volumes/kafka1/data kafka-cluster-volumes/kafka2/data kafka-cluster-volumes/kafka3/data
	docker run --rm -v /kafka-cluster-volumes:/data alpine chown -R 1000:1000 /data
	docker compose up -d --build

down:
	docker compose down

build:
	docker compose build

restart:
	docker compose restart

logs:
	docker compose logs -f $(SERVICE)

ps:
	docker compose ps

spark-submit:
	docker compose exec \
		-e ENV=$(ENV) \
		-e AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
		-e AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
		-e S3_ENDPOINT=$(S3_ENDPOINT) \
		spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/app/$(SPARK_LAYER)/$(SPARK_JOB).py

register-connector:
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d '{"name": "streaming-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "postgres", "database.port": "5432", "database.user": "postgres", "database.password": "postgres", "database.dbname": "postgres", "database.server.name": "adjust-dbserver", "slot.name": "debezium", "plugin.name": "pgoutput", "table.include.list": "adjust.event"}}'

connector-status:
	curl http://localhost:8083/connectors/streaming-connector/status
