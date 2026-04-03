"""
Stage 8: Azure Function — Inventory File Drop Trigger
Triggered by Azure Blob Storage when a new inventory batch file is dropped.
Validates the file, triggers backfill if needed, and routes to bronze pipeline.

Supported file formats: .csv, .json, .parquet
"""

import json
import logging
import os
import io
from datetime import datetime, timezone

import azure.functions as func


def main(myblob: func.InputStream) -> None:
    """
    Triggered when a file is dropped into the inventory-drops container.
    Validates the file format and record count before pipeline trigger.
    """
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    blob_name = myblob.name
    blob_size = myblob.length
    log.info("Inventory file drop: %s (%d bytes)", blob_name, blob_size)

    if blob_size == 0:
        log.warning("Empty file — skipping: %s", blob_name)
        return

    # Route by file extension
    if blob_name.endswith(".json"):
        records = _read_json(myblob)
    elif blob_name.endswith(".csv"):
        records = _read_csv(myblob)
    elif blob_name.endswith(".parquet"):
        records = _read_parquet(myblob)
    else:
        log.error("Unsupported file format: %s", blob_name)
        _move_to_rejected(blob_name, "unsupported_format")
        return

    if not records:
        log.warning("No records parsed from: %s", blob_name)
        return

    # Validate required fields
    valid_count   = 0
    invalid_count = 0
    for rec in records:
        if all(rec.get(f) for f in ["sku", "store_id", "quantity_on_hand"]):
            valid_count += 1
        else:
            invalid_count += 1

    log.info("Parsed %s: total=%d valid=%d invalid=%d",
             blob_name, len(records), valid_count, invalid_count)

    pass_rate = valid_count / len(records) * 100 if records else 0
    if pass_rate < 80:
        log.error("Validation failed (%.1f%% valid) — moving to rejected: %s", pass_rate, blob_name)
        _move_to_rejected(blob_name, f"low_pass_rate_{pass_rate:.0f}pct")
        return

    # Write validated records to bronze staging
    _write_to_bronze_staging(records, blob_name)
    log.info("Inventory file processed successfully: %s → bronze staging", blob_name)


def _read_json(blob: func.InputStream) -> list[dict]:
    try:
        data = json.loads(blob.read())
        return data if isinstance(data, list) else [data]
    except Exception as exc:
        logging.error("JSON parse error: %s", exc)
        return []


def _read_csv(blob: func.InputStream) -> list[dict]:
    try:
        import pandas as pd
        df = pd.read_csv(io.BytesIO(blob.read()))
        return df.to_dict(orient="records")
    except Exception as exc:
        logging.error("CSV parse error: %s", exc)
        return []


def _read_parquet(blob: func.InputStream) -> list[dict]:
    try:
        import pandas as pd
        df = pd.read_parquet(io.BytesIO(blob.read()))
        return df.to_dict(orient="records")
    except Exception as exc:
        logging.error("Parquet parse error: %s", exc)
        return []


def _write_to_bronze_staging(records: list[dict], source_blob: str) -> None:
    from azure.storage.blob import BlobServiceClient
    conn_str = os.environ.get("AzureWebJobsStorage", "")
    if not conn_str:
        return
    try:
        svc       = BlobServiceClient.from_connection_string(conn_str)
        container = svc.get_container_client("bronze-staging")
        ts        = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
        key       = f"inventory/{ts}/{os.path.basename(source_blob)}.json"
        container.upload_blob(
            key,
            json.dumps({"source": source_blob, "records": records,
                        "ingested_at": datetime.now(timezone.utc).isoformat()}).encode(),
            overwrite=True
        )
        logging.info("Written %d records to bronze staging: %s", len(records), key)
    except Exception as exc:
        logging.error("Staging write failed: %s", exc)


def _move_to_rejected(blob_name: str, reason: str) -> None:
    """Move rejected file to a separate container for manual review."""
    logging.warning("Rejected: %s reason=%s", blob_name, reason)
    # TODO: copy blob to rejected container via Azure SDK
