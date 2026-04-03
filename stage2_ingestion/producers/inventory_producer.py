"""
Stage 2: Inventory Event Producer
Simulates warehouse and in-store inventory update events.

Usage:
    python inventory_producer.py --events 500
"""

import argparse
import json
import logging
import random
import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from kafka import KafkaProducer
from kafka.errors import KafkaError
from stage1_infrastructure.config.settings import config
from stage2_ingestion.schemas.event_schemas import make_event_id, make_timestamp

log = logging.getLogger(__name__)

WAREHOUSES = [f"WH{i:02d}" for i in range(1, 6)]
SUPPLIERS = ["SUP-ACME", "SUP-GLOBAL", "SUP-DIRECT", "SUP-FRESH", "SUP-TECH"]
SKUS = [
    "ELEC-TV-001", "ELEC-LPT-002", "APRL-JNS-003", "APRL-TSH-004",
    "GROC-MLK-005", "GROC-BRD-006", "HOME-MOP-007", "BEAU-SHM-008",
    "SPRT-YGA-009", "TOYS-BLD-010", "AUTO-OIL-011", "ELEC-PHN-012",
    "GROC-EGG-013", "HOME-TWL-014", "BEAU-LPN-015",
]
LOCATIONS = [f"{chr(65 + i)}{j:02d}" for i in range(10) for j in range(1, 21)]


def build_inventory_event(store_id: str) -> dict:
    event_type = random.choices(
        ["restock", "sale_deduction", "adjustment", "transfer_in",
         "transfer_out", "damage", "return", "cycle_count"],
        weights=[20, 40, 10, 10, 5, 5, 7, 3]
    )[0]

    sku = random.choice(SKUS)
    on_hand = random.randint(0, 500)

    # Delta logic by event type
    if event_type in ("restock", "transfer_in", "return"):
        delta = random.randint(10, 200)
    elif event_type in ("sale_deduction", "transfer_out", "damage"):
        delta = -random.randint(1, min(on_hand, 50))
    elif event_type == "adjustment":
        delta = random.randint(-20, 20)
    else:  # cycle_count — delta is 0, just a count snapshot
        delta = 0

    return {
        "event_id":         make_event_id(),
        "event_type":       event_type,
        "store_id":         store_id,
        "warehouse_id":     random.choice(WAREHOUSES) if event_type in ("restock", "transfer_in") else None,
        "sku":              sku,
        "product_name":     f"Product {sku}",
        "timestamp":        make_timestamp(),
        "quantity_delta":   delta,
        "quantity_on_hand": max(0, on_hand + delta),
        "reorder_point":    random.randint(10, 50),
        "supplier_id":      random.choice(SUPPLIERS) if event_type == "restock" else None,
        "po_number":        f"PO-{random.randint(100000, 999999)}" if event_type == "restock" else None,
        "location":         random.choice(LOCATIONS),
        "metadata": {
            "schema_version": "1.0",
            "source_system":  "wms_v2",
            "ingestion_time": make_timestamp(),
        }
    }


def produce(num_events: int, tps: int, topic: str) -> None:
    logging.basicConfig(level=config.log_level, format="%(asctime)s %(levelname)s %(message)s")
    kc = config.kafka
    producer = KafkaProducer(
        bootstrap_servers=kc.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        batch_size=kc.producer_batch_size,
        linger_ms=kc.producer_linger_ms,
        compression_type=kc.producer_compression,
        acks=kc.producer_acks,
        retries=kc.producer_retries,
    )
    interval = 1.0 / tps if tps > 0 else 0
    sent, errors = 0, 0
    start = time.perf_counter()

    for _ in range(num_events):
        store_id = random.choice(config.store_ids)
        event = build_inventory_event(store_id)
        try:
            producer.send(topic, key=f"{store_id}:{event['sku']}", value=event)
            sent += 1
        except KafkaError as exc:
            errors += 1
            log.error("Send error: %s", exc)
        if interval > 0:
            time.sleep(interval)

    producer.flush()
    elapsed = time.perf_counter() - start
    log.info("Done. sent=%d errors=%d elapsed=%.2fs tps=%.0f", sent, errors, elapsed, sent / elapsed)
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inventory Event Producer")
    parser.add_argument("--events", type=int, default=500)
    parser.add_argument("--tps",    type=int, default=50)
    parser.add_argument("--topic",  type=str, default="retail.inventory.events")
    args = parser.parse_args()
    produce(args.events, args.tps, args.topic)
