"""
Stage 2: Retail Event Schemas
JSON schemas for POS, inventory, and e-commerce events.
Used by producers for generation and consumers for validation.
"""

from datetime import datetime, timezone
import uuid

# ─── POS Transaction Schema ──────────────────────────────────────────────────

POS_EVENT_SCHEMA = {
    "type": "object",
    "required": ["event_id", "event_type", "store_id", "terminal_id", "timestamp",
                 "transaction_id", "items", "payment", "totals"],
    "properties": {
        "event_id":       {"type": "string"},
        "event_type":     {"type": "string", "enum": ["sale", "refund", "void", "no_sale"]},
        "store_id":       {"type": "string"},
        "terminal_id":    {"type": "string"},
        "cashier_id":     {"type": "string"},
        "timestamp":      {"type": "string", "format": "date-time"},
        "transaction_id": {"type": "string"},
        "items": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["sku", "quantity", "unit_price", "line_total"],
                "properties": {
                    "sku":          {"type": "string"},
                    "product_name": {"type": "string"},
                    "category":     {"type": "string"},
                    "quantity":     {"type": "integer", "minimum": 1},
                    "unit_price":   {"type": "number", "minimum": 0},
                    "discount":     {"type": "number", "minimum": 0},
                    "line_total":   {"type": "number"},
                }
            }
        },
        "payment": {
            "type": "object",
            "required": ["method", "amount_tendered"],
            "properties": {
                "method":          {"type": "string"},
                "amount_tendered": {"type": "number"},
                "change_given":    {"type": "number"},
                "card_last_four":  {"type": "string"},
            }
        },
        "totals": {
            "type": "object",
            "required": ["subtotal", "tax", "total"],
            "properties": {
                "subtotal":  {"type": "number"},
                "discount":  {"type": "number"},
                "tax":       {"type": "number"},
                "total":     {"type": "number"},
            }
        },
        "metadata": {"type": "object"},
    }
}

# ─── Inventory Event Schema ───────────────────────────────────────────────────

INVENTORY_EVENT_SCHEMA = {
    "type": "object",
    "required": ["event_id", "event_type", "store_id", "sku", "timestamp", "quantity_delta", "quantity_on_hand"],
    "properties": {
        "event_id":          {"type": "string"},
        "event_type":        {"type": "string",
                              "enum": ["restock", "sale_deduction", "adjustment", "transfer_in",
                                       "transfer_out", "damage", "return", "cycle_count"]},
        "store_id":          {"type": "string"},
        "warehouse_id":      {"type": "string"},
        "sku":               {"type": "string"},
        "product_name":      {"type": "string"},
        "timestamp":         {"type": "string", "format": "date-time"},
        "quantity_delta":    {"type": "integer"},   # Positive = added, negative = removed
        "quantity_on_hand":  {"type": "integer", "minimum": 0},
        "reorder_point":     {"type": "integer"},
        "supplier_id":       {"type": "string"},
        "po_number":         {"type": "string"},
        "location":          {"type": "string"},   # Aisle/bin location
        "metadata":          {"type": "object"},
    }
}

# ─── E-Commerce Event Schema ──────────────────────────────────────────────────

ECOMMERCE_EVENT_SCHEMA = {
    "type": "object",
    "required": ["event_id", "event_type", "session_id", "customer_id", "timestamp"],
    "properties": {
        "event_id":     {"type": "string"},
        "event_type":   {"type": "string",
                         "enum": ["page_view", "add_to_cart", "remove_from_cart",
                                  "checkout_start", "order_placed", "order_cancelled",
                                  "return_initiated", "search"]},
        "session_id":   {"type": "string"},
        "customer_id":  {"type": "string"},
        "timestamp":    {"type": "string", "format": "date-time"},
        "channel":      {"type": "string", "enum": ["web", "mobile_app", "tablet"]},
        "sku":          {"type": ["string", "null"]},
        "product_name": {"type": ["string", "null"]},
        "category":     {"type": ["string", "null"]},
        "price":        {"type": ["number", "null"]},
        "quantity":     {"type": ["integer", "null"]},
        "order": {
            "type": ["object", "null"],
            "properties": {
                "order_id":      {"type": "string"},
                "items":         {"type": "array"},
                "subtotal":      {"type": "number"},
                "shipping":      {"type": "number"},
                "tax":           {"type": "number"},
                "total":         {"type": "number"},
                "payment_method": {"type": "string"},
            }
        },
        "search_query": {"type": ["string", "null"]},
        "device_type":  {"type": "string"},
        "metadata":     {"type": "object"},
    }
}

# ─── Helpers ─────────────────────────────────────────────────────────────────

def make_event_id() -> str:
    return str(uuid.uuid4())

def make_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()
