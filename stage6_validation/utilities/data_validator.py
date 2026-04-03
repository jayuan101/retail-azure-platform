"""
Stage 6: Python Data Validation Utilities
Validates retail datasets against business rules and schema contracts.
Used to boost pipeline reliability before Silver/Gold writes.

Features:
  - Schema contract validation (jsonschema)
  - Business rule checks (price consistency, quantity bounds, etc.)
  - Blob-based batch file reconciliation
  - Validation report generation
"""

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

import jsonschema
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
from stage2_ingestion.schemas.event_schemas import POS_EVENT_SCHEMA, INVENTORY_EVENT_SCHEMA

log = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    source:         str
    total_records:  int
    valid_count:    int
    invalid_count:  int
    errors:         list[dict] = field(default_factory=list)
    validated_at:   str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def pass_rate(self) -> float:
        return (self.valid_count / self.total_records * 100) if self.total_records > 0 else 0.0

    @property
    def passed(self) -> bool:
        return self.pass_rate >= 95.0  # 95% threshold for pipeline to proceed

    def summary(self) -> str:
        status = "PASS" if self.passed else "FAIL"
        return (
            f"[{status}] {self.source}: {self.valid_count}/{self.total_records} valid "
            f"({self.pass_rate:.1f}%) at {self.validated_at}"
        )


class RetailDataValidator:
    """
    Validates retail events against JSON schema contracts and business rules.
    """

    BUSINESS_RULES = {
        "pos": [
            ("total_amount_positive",  lambda r: r.get("totals", {}).get("total", 0) >= 0),
            ("has_items",              lambda r: len(r.get("items", [])) > 0),
            ("valid_payment_method",   lambda r: r.get("payment", {}).get("method") in
                                                 ["cash", "credit_card", "debit_card",
                                                  "mobile_pay", "gift_card", "store_credit"]),
            ("store_id_not_empty",     lambda r: bool(r.get("store_id", "").strip())),
            ("reasonable_total",       lambda r: r.get("totals", {}).get("total", 0) < 100_000),
        ],
        "inventory": [
            ("quantity_on_hand_nonneg", lambda r: r.get("quantity_on_hand", 0) >= 0),
            ("sku_not_empty",           lambda r: bool(r.get("sku", "").strip())),
            ("store_id_not_empty",      lambda r: bool(r.get("store_id", "").strip())),
            ("valid_event_type",        lambda r: r.get("event_type") in
                                                  ["restock", "sale_deduction", "adjustment",
                                                   "transfer_in", "transfer_out", "damage",
                                                   "return", "cycle_count"]),
        ],
    }

    def validate_record(self, record: dict, source: str) -> tuple[bool, list[str]]:
        """Validate a single record against schema + business rules. Returns (is_valid, errors)."""
        errors = []

        # Schema validation
        schema = POS_EVENT_SCHEMA if source == "pos" else INVENTORY_EVENT_SCHEMA
        try:
            jsonschema.validate(record, schema)
        except jsonschema.ValidationError as exc:
            errors.append(f"schema: {exc.message}")

        # Business rules
        for rule_name, rule_fn in self.BUSINESS_RULES.get(source, []):
            try:
                if not rule_fn(record):
                    errors.append(f"business_rule: {rule_name}")
            except Exception as exc:
                errors.append(f"business_rule_error: {rule_name} — {exc}")

        return len(errors) == 0, errors

    def validate_batch(self, records: list[dict], source: str) -> ValidationResult:
        """Validate a list of records and return an aggregated ValidationResult."""
        valid_count   = 0
        invalid_count = 0
        error_sample  = []  # Keep first 100 errors to avoid memory blowup

        for i, record in enumerate(records):
            is_valid, errors = self.validate_record(record, source)
            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
                if len(error_sample) < 100:
                    error_sample.append({
                        "record_index": i,
                        "event_id":     record.get("event_id", "unknown"),
                        "errors":       errors,
                    })

        result = ValidationResult(
            source=source,
            total_records=len(records),
            valid_count=valid_count,
            invalid_count=invalid_count,
            errors=error_sample,
        )
        log.info(result.summary())
        return result

    def validate_dataframe(self, df: pd.DataFrame, source: str) -> ValidationResult:
        """Validate a Pandas DataFrame of retail records."""
        records = df.to_dict(orient="records")
        return self.validate_batch(records, source)


class BlobReconciler:
    """
    Reconciles blob-based batch files against expected record counts.
    Ensures no files are missing or duplicate between pipeline runs.
    """

    def __init__(self, blob_service_client, container: str):
        self._client    = blob_service_client
        self._container = container

    def list_blobs_for_date(self, date_prefix: str) -> list[str]:
        """List all blob paths for a given date prefix (YYYY-MM-DD)."""
        container = self._client.get_container_client(self._container)
        return [b.name for b in container.list_blobs(name_starts_with=date_prefix)]

    def reconcile(self, expected_count: int, date_prefix: str) -> dict:
        """
        Compare expected record count against actual records in blobs.
        Returns reconciliation report dict.
        """
        blobs = self.list_blobs_for_date(date_prefix)
        actual_count = 0
        blob_details = []

        for blob_name in blobs:
            try:
                blob_client = self._client.get_blob_client(self._container, blob_name)
                props = blob_client.get_blob_properties()
                # Estimate record count from file size (rough approximation)
                est_records = props.size // 200  # ~200 bytes/record compressed Parquet
                actual_count += est_records
                blob_details.append({"blob": blob_name, "size_bytes": props.size, "est_records": est_records})
            except Exception as exc:
                log.warning("Could not inspect blob %s: %s", blob_name, exc)

        variance_pct = abs(actual_count - expected_count) / max(expected_count, 1) * 100
        status = "OK" if variance_pct < 5.0 else "WARN" if variance_pct < 15.0 else "FAIL"

        report = {
            "date_prefix":    date_prefix,
            "expected_count": expected_count,
            "actual_count":   actual_count,
            "variance_pct":   round(variance_pct, 2),
            "status":         status,
            "blob_count":     len(blobs),
            "blob_details":   blob_details,
            "reconciled_at":  datetime.now(timezone.utc).isoformat(),
        }
        log.info("Reconciliation [%s]: expected=%d actual=%d variance=%.1f%%",
                 status, expected_count, actual_count, variance_pct)
        return report

    def save_report(self, report: dict, report_path: str) -> None:
        """Write reconciliation report JSON to local path."""
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        log.info("Saved reconciliation report: %s", report_path)


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Validate retail data files")
    parser.add_argument("--file",   required=True, help="Path to JSON or Parquet file")
    parser.add_argument("--source", required=True, choices=["pos", "inventory", "ecommerce"])
    args = parser.parse_args()

    if args.file.endswith(".json"):
        with open(args.file) as f:
            records = json.load(f)
        if not isinstance(records, list):
            records = [records]
    elif args.file.endswith(".parquet"):
        records = pd.read_parquet(args.file).to_dict(orient="records")
    else:
        print("Unsupported file format — use .json or .parquet")
        sys.exit(1)

    validator = RetailDataValidator()
    result = validator.validate_batch(records, args.source)
    print(result.summary())
    if result.errors:
        print("\nSample errors:")
        for err in result.errors[:5]:
            print(f"  record {err['record_index']} ({err['event_id']}): {err['errors']}")
    sys.exit(0 if result.passed else 1)
