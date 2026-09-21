-include .env

postgres-up:
	docker compose up -d postgres
	sleep 5

kafka-up:
	mkdir -p kafka-cluster-volumes/kafka1/data kafka-cluster-volumes/kafka2/data kafka-cluster-volumes/kafka3/data
	docker run --rm -v $(CURDIR)/kafka-cluster-volumes:/data alpine chown -R 1000:1000 /data
	sleep 5
	docker compose up -d kafka1 kafka2 kafka3
	sleep 10
	docker compose up -d kconnect
	sleep 10
	docker compose up -d kafka-ui

register-connector:
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d '{"name": "streaming-connector", "config": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "postgres", "database.port": "5432", "database.user": "postgres", "database.password": "postgres", "database.dbname": "postgres", "database.server.name": "adjust-dbserver", "slot.name": "debezium", "plugin.name": "pgoutput", "table.include.list": "adjust.event"}}'

simulator-up:
	docker compose up -d simulator

minio-up:
	docker compose up -d minio minio-init
	sleep 5
	docker compose up -d spark-master spark-worker-1 spark-worker-2
	docker compose exec \
		-e ENV=$(ENV) \
		-e AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
		-e AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
		-e S3_ENDPOINT=$(S3_ENDPOINT) \
		spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/app/spark_builder.py

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
		/app/spark_builder.py

connector-status:
	curl http://localhost:8083/connectors/streaming-connector/status
