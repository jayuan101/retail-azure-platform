"""
Stage 1: Kafka Topic Setup
Creates optimized topics for retail event streams.
Partition counts are tuned for >150K events/sec throughput.
"""

import os
import time
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Topic configs tuned for high-volume retail workloads
TOPICS = [
    NewTopic(
        name="retail.pos.events",
        num_partitions=12,          # 12 partitions → ~12K events/sec per partition ceiling
        replication_factor=1,       # 1 for dev; use 3 in prod
        topic_configs={
            "retention.ms": str(24 * 60 * 60 * 1000),   # 24h retention
            "compression.type": "snappy",                 # Compress for throughput
            "min.insync.replicas": "1",
            "max.message.bytes": "1048576",               # 1 MB max message
        }
    ),
    NewTopic(
        name="retail.inventory.events",
        num_partitions=6,
        replication_factor=1,
        topic_configs={
            "retention.ms": str(48 * 60 * 60 * 1000),   # 48h — inventory needs longer window
            "compression.type": "snappy",
            "min.insync.replicas": "1",
        }
    ),
    NewTopic(
        name="retail.ecommerce.events",
        num_partitions=8,
        replication_factor=1,
        topic_configs={
            "retention.ms": str(24 * 60 * 60 * 1000),
            "compression.type": "snappy",
            "min.insync.replicas": "1",
        }
    ),
    NewTopic(
        name="retail.pos.dlq",           # Dead-letter queue for failed POS events
        num_partitions=3,
        replication_factor=1,
        topic_configs={
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),  # 7 days for investigation
        }
    ),
    NewTopic(
        name="retail.inventory.dlq",
        num_partitions=3,
        replication_factor=1,
        topic_configs={
            "retention.ms": str(7 * 24 * 60 * 60 * 1000),
        }
    ),
    NewTopic(
        name="retail.schema.changes",    # Schema change events for Hive Metastore sync
        num_partitions=1,
        replication_factor=1,
        topic_configs={
            "retention.ms": str(30 * 24 * 60 * 60 * 1000),  # 30 days
            "cleanup.policy": "compact",                       # Keep latest schema per key
        }
    ),
]


def wait_for_kafka(max_retries: int = 10, delay: int = 3) -> KafkaAdminClient:
    for attempt in range(max_retries):
        try:
            client = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id="retail-topic-setup",
                request_timeout_ms=5000,
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return client
        except Exception as exc:
            log.warning("Kafka not ready (attempt %d/%d): %s", attempt + 1, max_retries, exc)
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {BOOTSTRAP_SERVERS} after {max_retries} attempts")


def create_topics(admin: KafkaAdminClient) -> None:
    existing = set(admin.list_topics())
    new_topics = [t for t in TOPICS if t.name not in existing]

    if not new_topics:
        log.info("All topics already exist.")
        return

    try:
        result = admin.create_topics(new_topics=new_topics, validate_only=False)
        for topic, error in result.topic_errors:
            if error and error.errno != 0:
                log.error("Failed to create topic %s: %s", topic, error)
            else:
                log.info("Created topic: %s", topic)
    except TopicAlreadyExistsError:
        log.info("Topics already exist — skipping.")


def verify_topics(admin: KafkaAdminClient) -> None:
    existing = set(admin.list_topics())
    for topic in TOPICS:
        status = "OK" if topic.name in existing else "MISSING"
        log.info("[%s] %s (partitions=%d)", status, topic.name, topic.num_partitions)


if __name__ == "__main__":
    admin = wait_for_kafka()
    create_topics(admin)
    verify_topics(admin)
    admin.close()
    log.info("Topic setup complete.")
