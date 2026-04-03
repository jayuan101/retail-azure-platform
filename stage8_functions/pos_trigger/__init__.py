"""
Stage 8: Azure Function — POS Update Trigger
Triggered by Azure Event Grid or Blob Storage events when new POS data arrives.
Validates the event and kicks off the bronze ingestion pipeline via ADF.

Free tier: 1 million executions/month included.
Deploy: func azure functionapp publish <your-app-name>
"""

import json
import logging
import os
from datetime import datetime, timezone

import azure.functions as func

# Reuse validation utilities from stage6
# In production deployment, package these as a shared layer
def _validate_pos_event(event_data: dict) -> tuple[bool, str]:
    required = ["event_id", "store_id", "transaction_id", "timestamp", "totals"]
    missing  = [f for f in required if f not in event_data]
    if missing:
        return False, f"Missing required fields: {missing}"
    if event_data.get("totals", {}).get("total", -1) < 0:
        return False, "Negative total amount"
    if not event_data.get("store_id", "").strip():
        return False, "Empty store_id"
    return True, "OK"


def main(event: func.EventGridEvent) -> None:
    """
    Triggered when a POS event is published to Event Grid.
    Validates and routes to bronze ingestion or DLQ.
    """
    log = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

    log.info("POS trigger received: event_id=%s subject=%s", event.id, event.subject)

    try:
        event_data = event.get_json()
    except Exception as exc:
        log.error("Failed to parse event JSON: %s", exc)
        return

    is_valid, reason = _validate_pos_event(event_data)
    if not is_valid:
        log.warning("Invalid POS event %s → DLQ: %s", event.id, reason)
        _send_to_dlq(event_data, reason)
        return

    # Trigger downstream pipeline — in production use ADF REST API or Service Bus
    log.info("Valid POS event. store_id=%s transaction_id=%s total=%.2f",
             event_data.get("store_id"),
             event_data.get("transaction_id"),
             event_data.get("totals", {}).get("total", 0))

    # Enrich with function metadata
    event_data["_function_processed_at"] = datetime.now(timezone.utc).isoformat()
    event_data["_function_name"] = "pos_trigger"

    # Write to bronze staging blob
    _write_to_staging(event_data)

    log.info("POS event processed successfully: %s", event.id)


def _send_to_dlq(event_data: dict, reason: str) -> None:
    """Send invalid event to dead-letter storage blob."""
    from azure.storage.blob import BlobServiceClient
    conn_str = os.environ.get("AzureWebJobsStorage", "")
    if not conn_str:
        logging.warning("No storage connection — DLQ skipped.")
        return
    try:
        svc = BlobServiceClient.from_connection_string(conn_str)
        container = svc.get_container_client("pos-dlq")
        ts  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
        payload = json.dumps({"event": event_data, "reason": reason, "ts": ts})
        container.upload_blob(f"{ts}.json", payload.encode(), overwrite=True)
    except Exception as exc:
        logging.error("DLQ write failed: %s", exc)


def _write_to_staging(event_data: dict) -> None:
    """Write validated POS event to bronze staging blob for batch pickup."""
    from azure.storage.blob import BlobServiceClient
    conn_str = os.environ.get("AzureWebJobsStorage", "")
    if not conn_str:
        return
    try:
        svc = BlobServiceClient.from_connection_string(conn_str)
        container = svc.get_container_client("bronze-staging")
        ts  = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H")
        key = f"pos/{ts}/{event_data['event_id']}.json"
        container.upload_blob(key, json.dumps(event_data).encode(), overwrite=True)
    except Exception as exc:
        logging.error("Staging write failed: %s", exc)
