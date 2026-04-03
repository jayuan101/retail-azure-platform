"""
Generate sample data files for local testing without Kafka running.
Creates JSON files in data/raw/ that the bronze pipeline can process.

Usage:
    python data/sample/generate_samples.py --pos 1000 --inventory 500
"""

import argparse
import json
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from stage2_ingestion.producers.pos_producer import build_pos_event
from stage2_ingestion.producers.inventory_producer import build_inventory_event
from stage1_infrastructure.config.settings import config


def generate(pos_count: int, inv_count: int, output_dir: str) -> None:
    os.makedirs(f"{output_dir}/pos", exist_ok=True)
    os.makedirs(f"{output_dir}/inventory", exist_ok=True)

    # POS events
    pos_records = [build_pos_event(random.choice(config.store_ids)) for _ in range(pos_count)]
    pos_path = f"{output_dir}/pos/sample_pos_{pos_count}.json"
    with open(pos_path, "w") as f:
        json.dump(pos_records, f, indent=2)
    print(f"Generated {pos_count} POS events → {pos_path}")

    # Inventory events
    inv_records = [build_inventory_event(random.choice(config.store_ids)) for _ in range(inv_count)]
    inv_path = f"{output_dir}/inventory/sample_inventory_{inv_count}.json"
    with open(inv_path, "w") as f:
        json.dump(inv_records, f, indent=2)
    print(f"Generated {inv_count} inventory events → {inv_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pos",       type=int, default=1000)
    parser.add_argument("--inventory", type=int, default=500)
    parser.add_argument("--output",    type=str, default="./data/raw")
    args = parser.parse_args()
    generate(args.pos, args.inventory, args.output)
