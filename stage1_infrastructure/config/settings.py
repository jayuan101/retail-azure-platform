"""
Central configuration — reads from .env and provides typed settings.
"""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class KafkaConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic_pos: str = "retail.pos.events"
    topic_inventory: str = "retail.inventory.events"
    topic_ecommerce: str = "retail.ecommerce.events"
    topic_pos_dlq: str = "retail.pos.dlq"
    topic_inventory_dlq: str = "retail.inventory.dlq"
    consumer_group: str = os.getenv("KAFKA_CONSUMER_GROUP", "retail-consumer-group")

    # Producer tuning for high throughput (>150K events/sec target)
    producer_batch_size: int = 65536          # 64 KB batches
    producer_linger_ms: int = 5               # Wait 5ms to batch more messages
    producer_compression: str = "snappy"
    producer_acks: str = "1"                  # Async ack — balance speed vs durability
    producer_retries: int = 3
    producer_max_block_ms: int = 60000

    # Consumer tuning
    consumer_max_poll_records: int = 500
    consumer_fetch_min_bytes: int = 1024
    consumer_fetch_max_wait_ms: int = 500
    consumer_session_timeout_ms: int = 30000
    consumer_auto_offset_reset: str = "earliest"
    consumer_enable_auto_commit: bool = False  # Manual commit for exactly-once semantics


@dataclass
class AzureStorageConfig:
    connection_string: str = field(
        default_factory=lambda: os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING",
            # Azurite local emulator connection string
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tiqIpBA==;"
            "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
        )
    )
    container_bronze: str = os.getenv("AZURE_CONTAINER_BRONZE", "bronze")
    container_silver: str = os.getenv("AZURE_CONTAINER_SILVER", "silver")
    container_gold: str = os.getenv("AZURE_CONTAINER_GOLD", "gold")
    container_checkpoints: str = "checkpoints"
    use_local: bool = os.getenv("USE_LOCAL_STORAGE", "true").lower() == "true"


@dataclass
class EventHubConfig:
    connection_string: str = field(
        default_factory=lambda: os.getenv("EVENT_HUB_CONNECTION_STRING", "")
    )
    namespace: str = os.getenv("EVENT_HUB_NAMESPACE", "retail-eventhub-ns")
    hub_pos: str = os.getenv("EVENT_HUB_POS", "pos-events")
    hub_inventory: str = os.getenv("EVENT_HUB_INVENTORY", "inventory-events")
    hub_ecommerce: str = os.getenv("EVENT_HUB_ECOMMERCE", "ecommerce-events")


@dataclass
class RetailConfig:
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    storage: AzureStorageConfig = field(default_factory=AzureStorageConfig)
    event_hubs: EventHubConfig = field(default_factory=EventHubConfig)
    use_local_kafka: bool = os.getenv("USE_LOCAL_KAFKA", "true").lower() == "true"
    local_data_path: str = os.getenv("LOCAL_DATA_PATH", "./data")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # Retail domain constants
    store_ids: list = field(default_factory=lambda: [f"STR{i:04d}" for i in range(1, 51)])
    product_categories: list = field(default_factory=lambda: [
        "electronics", "apparel", "grocery", "home", "beauty", "sports", "toys", "automotive"
    ])
    payment_methods: list = field(default_factory=lambda: [
        "cash", "credit_card", "debit_card", "mobile_pay", "gift_card", "store_credit"
    ])


# Singleton config instance
config = RetailConfig()
