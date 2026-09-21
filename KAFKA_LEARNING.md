# Kafka Learning Roadmap

Lộ trình các phần Kafka còn cần học, dựa trên project `adjust-streaming-project`
(Postgres → Debezium CDC → Kafka → Spark → Iceberg/MinIO).

## Đã nắm được (qua project này)

- Dựng cluster Kafka multi-broker chạy KRaft mode (không cần Zookeeper)
- Replication factor, min.insync.replicas, ISR
- Log retention & segment (thời gian giữ log, khi nào Kafka xoá dữ liệu cũ)
- Kafka Connect + Debezium CDC connector (đọc thay đổi từ Postgres đẩy vào Kafka topic)
- Giám sát bằng Kafka UI
- Một số vấn đề vận hành thực tế: dependency giữa service, bind mount, docker compose start vs up

## Còn cần học

### 1. Producer/Consumer API (ưu tiên học trước)

- Message được ghi vào partition thế nào (key → hash → partition)
- Delivery guarantees: at-least-once, at-most-once, exactly-once
- Ack config: `acks=all`, `acks=1`, `acks=0`
- **Thực hành:** viết script Python (`confluent-kafka` hoặc `kafka-python`) consume
  topic CDC mà Debezium đang đẩy vào, in ra message. Viết thêm producer test để
  hiểu key/partition ảnh hưởng ra sao.

### 2. Consumer Group & Partitioning

- Consumer group hoạt động thế nào, rebalancing, offset commit (auto vs manual)
- `KAFKA_NUM_PARTITIONS=4` (đã cấu hình trong docker-compose) ảnh hưởng thế nào
  đến độ song song khi consume
- **Thực hành:** chạy 2 consumer cùng group đọc cùng topic 4 partitions, quan sát
  Kafka UI xem partition được chia cho consumer nào.

### 3. Schema Registry / Avro

- Vì sao cần schema (tránh vỡ dữ liệu khi Postgres đổi cấu trúc bảng)
- Debezium hỗ trợ Avro qua Confluent Schema Registry
- **Thực hành:** thêm service `schema-registry` vào docker-compose, đổi connector
  config Debezium dùng Avro converter thay vì JSON, xem topic message thay đổi
  thế nào.

### 4. Kafka Streams / Stream Processing

- Project đã dùng Spark Structured Streaming đọc Kafka (`spark-submit` trong
  Makefile) — đây là hướng tốt để học stream processing thực tế: windowing,
  watermark, checkpoint.
- Không nhất thiết phải học riêng Kafka Streams (thư viện Java) nếu đi theo
  hướng Spark.

### 5. Vận hành nâng cao (học sau)

- Security: SASL/SSL/ACL
- Monitoring: JMX metrics
- Tuning: `num.io.threads`, `segment.bytes`, v.v.
- Multi-datacenter replication (MirrorMaker)

## Gợi ý thứ tự học

1. Producer/Consumer API
2. Consumer Group & Partitioning
3. Schema Registry / Avro
4. Stream processing qua Spark (đã có sẵn hạ tầng)
5. Vận hành nâng cao khi cần
