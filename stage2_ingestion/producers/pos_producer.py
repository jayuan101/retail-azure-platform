"""
Stage 2: POS Event Producer
Generates realistic POS transaction events at configurable throughput.
Target: handles >150K events/sec with batching + compression.

Usage:
    python pos_producer.py --events 1000 --tps 500
    python pos_producer.py --events 100000 --tps 5000 --topic retail.pos.events
"""

import argparse
import json
import logging
import random
import sys
import time
import os
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from kafka import KafkaProducer
from kafka.errors import KafkaError
from faker import Faker
from stage1_infrastructure.config.settings import config
from stage2_ingestion.schemas.event_schemas import make_event_id, make_timestamp

log = logging.getLogger(__name__)
fake = Faker()

# ─── Data fixtures ────────────────────────────────────────────────────────────

PRODUCTS = [
    {"sku": "ELEC-TV-001", "name": "55in Smart TV",         "category": "electronics", "price": 499.99},
    {"sku": "ELEC-LPT-002", "name": "15in Laptop",          "category": "electronics", "price": 899.00},
    {"sku": "APRL-JNS-003", "name": "Men's Slim Jeans",     "category": "apparel",     "price": 49.99},
    {"sku": "APRL-TSH-004", "name": "Women's V-neck Tee",   "category": "apparel",     "price": 19.99},
    {"sku": "GROC-MLK-005", "name": "Whole Milk 1 Gal",     "category": "grocery",     "price": 3.49},
    {"sku": "GROC-BRD-006", "name": "Whole Wheat Bread",    "category": "grocery",     "price": 3.99},
    {"sku": "HOME-MOP-007", "name": "Spin Mop Set",         "category": "home",        "price": 34.99},
    {"sku": "BEAU-SHM-008", "name": "Shampoo 16oz",         "category": "beauty",      "price": 8.99},
    {"sku": "SPRT-YGA-009", "name": "Yoga Mat Premium",     "category": "sports",      "price": 44.99},
    {"sku": "TOYS-BLD-010", "name": "Building Blocks Set",  "category": "toys",        "price": 24.99},
    {"sku": "AUTO-OIL-011", "name": "5W-30 Motor Oil 5qt",  "category": "automotive",  "price": 22.99},
    {"sku": "ELEC-PHN-012", "name": "Smartphone Case",      "category": "electronics", "price": 14.99},
    {"sku": "GROC-EGG-013", "name": "Eggs 12ct",            "category": "grocery",     "price": 4.99},
    {"sku": "HOME-TWL-014", "name": "Bath Towel Set",       "category": "home",        "price": 29.99},
    {"sku": "BEAU-LPN-015", "name": "Lip Gloss Pack",       "category": "beauty",      "price": 12.99},
]


def build_pos_event(store_id: str) -> dict[str, Any]:
    """Generate one realistic POS transaction event."""
    event_type = random.choices(
        ["sale", "refund", "void", "no_sale"],
        weights=[85, 10, 3, 2]
    )[0]

    num_items = random.randint(1, 8)
    items = []
    subtotal = 0.0

    for _ in range(num_items):
        product = random.choice(PRODUCTS)
        qty = random.randint(1, 3)
        unit_price = product["price"] * random.uniform(0.95, 1.05)  # ±5% price variance
        discount = round(unit_price * random.choice([0, 0, 0, 0.05, 0.10, 0.15]), 2)
        line_total = round((unit_price - discount) * qty, 2)
        subtotal += line_total
        items.append({
            "sku":          product["sku"],
            "product_name": product["name"],
            "category":     product["category"],
            "quantity":     qty,
            "unit_price":   round(unit_price, 2),
            "discount":     discount,
            "line_total":   line_total,
        })

    subtotal = round(subtotal, 2)
    tax = round(subtotal * 0.0875, 2)  # 8.75% tax rate
    total = round(subtotal + tax, 2)
    payment_method = random.choice(config.payment_methods)

    return {
        "event_id":       make_event_id(),
        "event_type":     event_type,
        "store_id":       store_id,
        "terminal_id":    f"TRM-{random.randint(1, 20):03d}",
        "cashier_id":     f"EMP-{random.randint(1000, 9999)}",
        "timestamp":      make_timestamp(),
        "transaction_id": f"TXN-{fake.bothify('??####??####')}",
        "items":          items,
        "payment": {
            "method":           payment_method,
            "amount_tendered":  round(total + random.choice([0, 0.01, 0.25, 0.50]), 2),
            "change_given":     0.00,
            "card_last_four":   fake.credit_card_number()[-4:] if "card" in payment_method else None,
        },
        "totals": {
            "subtotal": subtotal,
            "discount": round(sum(i["discount"] * i["quantity"] for i in items), 2),
            "tax":      tax,
            "total":    total,
        },
        "metadata": {
            "schema_version": "1.0",
            "source_system":  "pos_v3",
            "ingestion_time": make_timestamp(),
        }
    }


def make_producer() -> KafkaProducer:
    kc = config.kafka
    return KafkaProducer(
        bootstrap_servers=kc.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        batch_size=kc.producer_batch_size,
        linger_ms=kc.producer_linger_ms,
        compression_type=kc.producer_compression,
        acks=kc.producer_acks,
        retries=kc.producer_retries,
        max_block_ms=kc.producer_max_block_ms,
    )


def produce(num_events: int, tps: int, topic: str, dry_run: bool = False) -> None:
    """
    Produce `num_events` POS events to Kafka at up to `tps` events/sec.
    Uses store_id as the partition key for co-location of store events.
    """
    logging.basicConfig(level=config.log_level, format="%(asctime)s %(levelname)s %(message)s")

    if dry_run:
        sample = build_pos_event(random.choice(config.store_ids))
        print(json.dumps(sample, indent=2))
        return

    producer = make_producer()
    interval = 1.0 / tps if tps > 0 else 0
    sent = 0
    errors = 0
    start = time.perf_counter()

    log.info("Producing %d POS events to topic '%s' at %d tps...", num_events, topic, tps)

    for _ in range(num_events):
        store_id = random.choice(config.store_ids)
        event = build_pos_event(store_id)

        try:
            producer.send(topic, key=store_id, value=event)
            sent += 1

            if sent % 1000 == 0:
                elapsed = time.perf_counter() - start
                actual_tps = sent / elapsed if elapsed > 0 else 0
                log.info("Sent %d/%d events | actual tps=%.0f", sent, num_events, actual_tps)

        except KafkaError as exc:
            errors += 1
            log.error("Send error: %s", exc)

        if interval > 0:
            time.sleep(interval)

    producer.flush()
    elapsed = time.perf_counter() - start
    log.info(
        "Done. Sent=%d errors=%d elapsed=%.2fs actual_tps=%.0f",
        sent, errors, elapsed, sent / elapsed if elapsed > 0 else 0
    )
    producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="POS Event Producer")
    parser.add_argument("--events", type=int, default=1000, help="Number of events to produce")
    parser.add_argument("--tps",    type=int, default=100,  help="Target events/sec (0=unlimited)")
    parser.add_argument("--topic",  type=str, default="retail.pos.events")
    parser.add_argument("--dry-run", action="store_true", help="Print one sample event and exit")
    args = parser.parse_args()

    produce(args.events, args.tps, args.topic, args.dry_run)
