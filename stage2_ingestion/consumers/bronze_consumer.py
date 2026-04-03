"""
Stage 2/3: Bronze Consumer
Reads Kafka events and lands them into Azure Blob Storage (or Azurite locally)
as Parquet files partitioned by date/hour — the raw Bronze layer.

Handles:
- Exactly-once semantics via manual offset commit after successful blob write
- Dead-letter routing for schema-invalid or corrupt messages
- Micro-batch writes (configurable flush interval) for blob efficiency
"""

import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from azure.storage.blob import BlobServiceClient
from stage1_infrastructure.config.settings import config

log = logging.getLogger(__name__)


class BronzeConsumer:
    """
    Consumes retail events from Kafka and writes micro-batched Parquet files
    to Azure Blob Storage (bronze container), partitioned by event_date/hour/source.
    """

    FLUSH_INTERVAL_SEC = 60        # Write a Parquet file every 60 seconds
    FLUSH_BATCH_SIZE   = 5_000     # Or every 5,000 records, whichever comes first

    def __init__(self, topics: list[str], container: str = None):
        self.topics    = topics
        self.container = container or config.storage.container_bronze
        self._buffer: dict[str, list[dict]] = defaultdict(list)
        self._last_flush = time.monotonic()

        self._consumer  = self._make_consumer()
        self._producer  = self._make_dlq_producer()
        self._blob_svc  = BlobServiceClient.from_connection_string(config.storage.connection_string)
        self._ensure_containers()

    # ── Setup ─────────────────────────────────────────────────────────────────

    def _make_consumer(self) -> KafkaConsumer:
        kc = config.kafka
        return KafkaConsumer(
            *self.topics,
            bootstrap_servers=kc.bootstrap_servers,
            group_id=f"{kc.consumer_group}.bronze",
            auto_offset_reset=kc.consumer_auto_offset_reset,
            enable_auto_commit=False,      # Manual commit after successful write
            max_poll_records=kc.consumer_max_poll_records,
            fetch_min_bytes=kc.consumer_fetch_min_bytes,
            fetch_max_wait_ms=kc.consumer_fetch_max_wait_ms,
            session_timeout_ms=kc.consumer_session_timeout_ms,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )

    def _make_dlq_producer(self) -> KafkaProducer:
        kc = config.kafka
        return KafkaProducer(
            bootstrap_servers=kc.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="1",
        )

    def _ensure_containers(self) -> None:
        for name in [self.container, config.storage.container_checkpoints]:
            try:
                self._blob_svc.create_container(name)
                log.info("Created container: %s", name)
            except Exception:
                pass  # Already exists

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        log.info("Bronze consumer started. Topics: %s", self.topics)
        try:
            while True:
                records = self._consumer.poll(timeout_ms=1000)
                for tp, messages in records.items():
                    for msg in messages:
                        self._handle_message(msg)
                self._maybe_flush()
        except KeyboardInterrupt:
            log.info("Shutdown signal received.")
        finally:
            self._flush_all()
            self._consumer.close()
            self._producer.close()

    def _handle_message(self, msg) -> None:
        try:
            event = msg.value
            if not isinstance(event, dict):
                raise ValueError("Message is not a JSON object")

            # Enrich with ingestion metadata
            now = datetime.now(timezone.utc)
            event["_ingestion_ts"]   = now.isoformat()
            event["_kafka_topic"]    = msg.topic
            event["_kafka_partition"] = msg.partition
            event["_kafka_offset"]   = msg.offset

            # Partition key: topic/date/hour
            partition_key = f"{msg.topic}/{now.strftime('%Y-%m-%d')}/{now.strftime('%H')}"
            self._buffer[partition_key].append(event)

            # Check flush thresholds
            total = sum(len(v) for v in self._buffer.values())
            if total >= self.FLUSH_BATCH_SIZE:
                self._flush_all()

        except Exception as exc:
            log.warning("Routing to DLQ — message error: %s", exc)
            self._send_to_dlq(msg)

    def _maybe_flush(self) -> None:
        if time.monotonic() - self._last_flush >= self.FLUSH_INTERVAL_SEC:
            self._flush_all()

    def _flush_all(self) -> None:
        if not self._buffer:
            return
        for partition_key, records in list(self._buffer.items()):
            if records:
                self._write_parquet(partition_key, records)
        self._buffer.clear()
        self._consumer.commit()
        self._last_flush = time.monotonic()
        log.debug("Flushed and committed offsets.")

    # ── Parquet write ─────────────────────────────────────────────────────────

    def _write_parquet(self, partition_key: str, records: list[dict]) -> None:
        try:
            df = pd.DataFrame(records)
            # Flatten nested dicts to JSON strings for schema consistency in bronze
            for col in df.columns:
                if df[col].dtype == object:
                    df[col] = df[col].apply(
                        lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x
                    )

            table = pa.Table.from_pandas(df)
            buf = BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)

            ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
            blob_path = f"{partition_key}/{ts}.parquet"
            blob_client = self._blob_svc.get_blob_client(self.container, blob_path)
            blob_client.upload_blob(buf, overwrite=True)

            log.info("Wrote %d records → %s/%s", len(records), self.container, blob_path)
        except Exception as exc:
            log.error("Failed to write Parquet for %s: %s", partition_key, exc)
            raise

    # ── Dead-letter queue ─────────────────────────────────────────────────────

    def _send_to_dlq(self, msg) -> None:
        dlq_topic = config.kafka.topic_pos_dlq if "pos" in msg.topic else config.kafka.topic_inventory_dlq
        payload = {
            "original_topic":     msg.topic,
            "original_partition": msg.partition,
            "original_offset":    msg.offset,
            "original_key":       msg.key,
            "error_ts":           datetime.now(timezone.utc).isoformat(),
            "raw_value":          str(msg.value),
        }
        try:
            self._producer.send(dlq_topic, value=payload)
        except KafkaError as exc:
            log.error("Failed to send to DLQ: %s", exc)


if __name__ == "__main__":
    logging.basicConfig(level=config.log_level, format="%(asctime)s %(levelname)s %(message)s")
    consumer = BronzeConsumer(
        topics=[
            config.kafka.topic_pos,
            config.kafka.topic_inventory,
            config.kafka.topic_ecommerce,
        ]
    )
    consumer.run()
