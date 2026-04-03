"""
Stage 6: SKU/Item Metadata API Ingestion
Fetches product and SKU metadata from internal/external APIs,
validates it, and upserts into the Silver product dimension table.

Supports:
  - REST API polling with retry/backoff
  - Incremental ingestion (last_modified timestamp watermark)
  - Schema validation before write
  - Blob checkpoint for watermark persistence
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from stage1_infrastructure.config.settings import config

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Schemas ─────────────────────────────────────────────────────────────────

SKU_RECORD_SCHEMA = {
    "type": "object",
    "required": ["sku", "product_name", "category", "unit_price", "active"],
    "properties": {
        "sku":            {"type": "string"},
        "product_name":   {"type": "string"},
        "category":       {"type": "string"},
        "subcategory":    {"type": ["string", "null"]},
        "brand":          {"type": ["string", "null"]},
        "unit_price":     {"type": "number", "minimum": 0},
        "cost_price":     {"type": ["number", "null"]},
        "unit_of_measure": {"type": "string"},
        "weight_kg":      {"type": ["number", "null"]},
        "is_perishable":  {"type": "boolean"},
        "active":         {"type": "boolean"},
        "created_at":     {"type": "string"},
        "updated_at":     {"type": "string"},
    }
}


# ─── Mock API (for local/free-tier dev) ──────────────────────────────────────

def generate_mock_sku_data(page: int = 1, page_size: int = 50) -> list[dict]:
    """
    Returns mock SKU data — replace with real API call in production.
    Simulates a paginated /api/v1/products endpoint.
    """
    import random
    from faker import Faker
    fake = Faker()

    categories = config.product_categories
    base_skus = [
        "ELEC-TV-001", "ELEC-LPT-002", "APRL-JNS-003", "APRL-TSH-004",
        "GROC-MLK-005", "GROC-BRD-006", "HOME-MOP-007", "BEAU-SHM-008",
        "SPRT-YGA-009", "TOYS-BLD-010", "AUTO-OIL-011", "ELEC-PHN-012",
        "GROC-EGG-013", "HOME-TWL-014", "BEAU-LPN-015",
    ]

    records = []
    start_idx = (page - 1) * page_size
    for i in range(start_idx, start_idx + page_size):
        sku = base_skus[i % len(base_skus)] + f"-{i:04d}"
        cat = random.choice(categories)
        price = round(random.uniform(2.99, 999.99), 2)
        records.append({
            "sku":             sku,
            "product_name":    fake.catch_phrase(),
            "category":        cat,
            "subcategory":     fake.word(),
            "brand":           fake.company(),
            "unit_price":      price,
            "cost_price":      round(price * random.uniform(0.4, 0.7), 2),
            "unit_of_measure": random.choice(["EA", "KG", "LB", "CASE", "PACK"]),
            "weight_kg":       round(random.uniform(0.1, 20.0), 2),
            "is_perishable":   cat == "grocery",
            "active":          random.random() > 0.05,
            "created_at":      "2024-01-01T00:00:00Z",
            "updated_at":      datetime.now(timezone.utc).isoformat(),
        })
    return records


class SkuApiIngestion:
    """
    Fetches SKU/item metadata from a REST API and writes to blob storage
    as a Parquet product dimension file.
    """

    CHECKPOINT_KEY = "sku_ingestion_watermark.json"

    def __init__(self, api_base_url: str = None, api_key: str = None):
        self.api_base_url = api_base_url or os.getenv("SKU_API_URL", "")
        self.api_key      = api_key or os.getenv("SKU_API_KEY", "")
        self._session     = self._make_session()
        self._watermark   = self._load_watermark()

    def _make_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        if self.api_key:
            session.headers.update({"Authorization": f"Bearer {self.api_key}"})
        return session

    def _load_watermark(self) -> Optional[str]:
        """Load last-run watermark from local checkpoint file."""
        checkpoint_path = os.path.join(config.local_data_path, self.CHECKPOINT_KEY)
        if os.path.exists(checkpoint_path):
            with open(checkpoint_path) as f:
                data = json.load(f)
                return data.get("last_updated_at")
        return None

    def _save_watermark(self, watermark: str) -> None:
        checkpoint_path = os.path.join(config.local_data_path, self.CHECKPOINT_KEY)
        os.makedirs(os.path.dirname(checkpoint_path), exist_ok=True)
        with open(checkpoint_path, "w") as f:
            json.dump({"last_updated_at": watermark, "saved_at": datetime.now(timezone.utc).isoformat()}, f)
        log.info("Watermark saved: %s", watermark)

    def fetch_page(self, page: int, page_size: int = 100) -> list[dict]:
        """Fetch one page of SKU data from API (or mock for local dev)."""
        if not self.api_base_url:
            # Local dev — use mock data
            return generate_mock_sku_data(page=page, page_size=page_size)

        url = f"{self.api_base_url}/api/v1/products"
        params = {"page": page, "page_size": page_size}
        if self._watermark:
            params["updated_since"] = self._watermark

        response = self._session.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json().get("data", [])

    def validate_records(self, records: list[dict]) -> tuple[list, list]:
        """Validate SKU records against schema. Returns (valid, invalid)."""
        import jsonschema
        valid, invalid = [], []
        for rec in records:
            try:
                jsonschema.validate(rec, SKU_RECORD_SCHEMA)
                valid.append(rec)
            except jsonschema.ValidationError as exc:
                rec["_validation_error"] = exc.message
                invalid.append(rec)
        return valid, invalid

    def save_to_parquet(self, records: list[dict], output_path: str) -> None:
        """Write validated SKU records to Parquet file."""
        import pandas as pd
        df = pd.DataFrame(records)
        df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False, compression="snappy")
        log.info("Saved %d SKU records → %s", len(records), output_path)

    def run(self, max_pages: int = 10, page_size: int = 100) -> dict:
        """
        Full ingestion run — paginate API, validate, write Parquet.
        Returns summary dict for pipeline reporting.
        """
        log.info("Starting SKU API ingestion. watermark=%s", self._watermark)
        all_valid    = []
        all_invalid  = []
        pages_fetched = 0

        for page in range(1, max_pages + 1):
            records = self.fetch_page(page, page_size)
            if not records:
                log.info("No more records at page %d — stopping.", page)
                break

            valid, invalid = self.validate_records(records)
            all_valid.extend(valid)
            all_invalid.extend(invalid)
            pages_fetched += 1
            log.info("Page %d: valid=%d invalid=%d", page, len(valid), len(invalid))
            time.sleep(0.1)  # Rate limiting courtesy

        # Write valid records
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        output_path = os.path.join(
            config.local_data_path, "silver", "product_dimension", f"sku_metadata_{today}.parquet"
        )
        if all_valid:
            self.save_to_parquet(all_valid, output_path)

        # Write invalid records to quarantine
        if all_invalid:
            invalid_path = os.path.join(
                config.local_data_path, "quarantine", "sku_api", f"invalid_{today}.parquet"
            )
            self.save_to_parquet(all_invalid, invalid_path)

        # Update watermark
        new_watermark = datetime.now(timezone.utc).isoformat()
        self._save_watermark(new_watermark)

        summary = {
            "pages_fetched":   pages_fetched,
            "valid_records":   len(all_valid),
            "invalid_records": len(all_invalid),
            "output_path":     output_path,
            "watermark":       new_watermark,
            "status":          "success" if len(all_valid) > 0 else "no_data",
        }
        log.info("SKU ingestion complete: %s", summary)
        return summary


if __name__ == "__main__":
    ingestion = SkuApiIngestion()
    summary = ingestion.run(max_pages=5, page_size=50)
    print(json.dumps(summary, indent=2))
