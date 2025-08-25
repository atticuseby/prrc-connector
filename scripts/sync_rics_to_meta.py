import csv
import os
import time
import json
import hashlib
import requests
import re
from collections import defaultdict
from datetime import datetime, timezone

# =========================
# Constants / Config
# =========================
DATASET_ID = os.getenv("META_DATASET_ID", "855183627077424")
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
API_URL = f"https://graph.facebook.com/v19.0/{DATASET_ID}/events"
HEADERS = {"Content-Type": "application/json"}

INPUT_CSV_PATH = "optimizely_connector/output/rics_customer_purchase_history_latest.csv"
BATCH_SIZE = 100
CURRENCY = "USD"
COUNTRY_DEFAULT = "US"

# =========================
# Helpers
# =========================
def sha256_norm(value: str) -> str | None:
    """Lowercase, trim, and SHA-256 hash a string. Returns None if empty."""
    if not value:
        return None
    v = value.strip().lower()
    if not v:
        return None
    return hashlib.sha256(v.encode("utf-8")).hexdigest()

def to_e164(phone: str) -> str | None:
    """Normalize to E.164 (assume US if 10 digits). Return None if cannot normalize."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 10:
        digits = "1" + digits  # assume US
    if len(digits) < 11 or len(digits) > 15:
        return None
    return "+" + digits

def sha256_phone(phone: str) -> str | None:
    e164 = to_e164(phone)
    return hashlib.sha256(e164.encode("utf-8")).hexdigest() if e164 else None

def to_epoch(dt_string: str) -> int:
    """Parse common RICS datetime strings to UTC epoch seconds; fallback to now."""
    if not dt_string:
        return int(time.time())
    fmts = ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M")
    for fmt in fmts:
        try:
            dt = datetime.strptime(dt_string.strip(), fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            continue
    # Last resort: current time
    return int(time.time())

def booly(v) -> bool:
    """Interpret common truthy strings for RICS flags."""
    return str(v).strip().lower() in {"y", "yes", "true", "1"}

def safe_float(v) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0

def safe_int(v) -> int:
    try:
        return int(float(v))
    except Exception:
        return 0

# =========================
# Build Events (grouped by TicketNumber)
# =========================
def build_events_from_csv(csv_path: str) -> list[dict]:
    """
    Group rows by TicketNumber (order). For each order:
      - event_name: Purchase
      - event_time: SaleDateTime (fallback TicketDateTime)
      - event_id: purchase-{TicketNumber}
      - custom_data: value (sum AmountPaid), currency, order_id, contents[]
      - match_keys: hashed PII + location + external_id (rics_id) if present
    """
    tickets: dict[str, list[dict]] = defaultdict(list)

    with open(csv_path, mode="r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip voided or suspended lines at the row level (conservative)
            if booly(row.get("TicketVoidedPrinted")) or booly(row.get("TicketVoided")):
                continue
            if booly(row.get("TicketSuspended")):
                continue

            ticket_no = (row.get("TicketNumber") or "").strip()
            if not ticket_no:
                # No order id, skip
                continue

            tickets[ticket_no].append(row)

    events: list[dict] = []

    for ticket_no, lines in tickets.items():
        # Determine event_time
        first = lines[0]
        sale_dt = first.get("SaleDateTime") or first.get("TicketDateTime")
        event_time = to_epoch(sale_dt)

        # Sum order value and build contents
        total_value = 0.0
        contents = []
        for ln in lines:
            qty = safe_float(ln.get("Quantity"))
            amt = safe_float(ln.get("AmountPaid"))
            if amt > 0:
                total_value += amt

            # Derive unit price from AmountPaid if it's a line total
            unit_price = amt / qty if qty > 0 else amt
            contents.append({
                "id": (ln.get("Sku") or "UNKNOWN").strip(),
                "quantity": safe_int(qty) if qty > 0 else 1,
                "item_price": round(unit_price, 2)
            })

        # Skip zero/negative orders
        if total_value <= 0:
            continue

        # Skip if the ticket/order itself is flagged (double-check)
        if booly(first.get("TicketVoidedPrinted")) or booly(first.get("TicketVoided")) or booly(first.get("TicketSuspended")):
            continue

        # Build match_keys from the first row (customer-level fields)
        mk = {
            "em": sha256_norm(first.get("email")),
            "ph": sha256_phone(first.get("phone")),
            "fn": sha256_norm(first.get("first_name")),
            "ln": sha256_norm(first.get("last_name")),
            "ct": sha256_norm(first.get("city")),
            "st": sha256_norm(first.get("state")),
            "zip": sha256_norm(str(first.get("zip") or "")),
            "country": sha256_norm(COUNTRY_DEFAULT),
            "external_id": sha256_norm(first.get("rics_id")),
        }
        # Strip empties
        match_keys = {k: v for k, v in mk.items() if v}

        event = {
            "event_name": "Purchase",
            "event_time": event_time,
            "event_id": f"purchase-{ticket_no}",
            "event_source": "offline",  # a.k.a. action_source for CAPI Web; accepted for Offline sets
            "match_keys": match_keys,
            "custom_data": {
                "order_id": str(ticket_no),
                "value": round(total_value, 2),
                "currency": CURRENCY,
                "contents": contents,
            },
        }
        events.append(event)

    return events

# =========================
# Sender
# =========================
def send_batch(events: list[dict]) -> None:
    payload = {
        "data": events,
        "access_token": ACCESS_TOKEN
    }
    resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
    if resp.ok:
        print(f"✅ Sent batch of {len(events)} events")
    else:
        print(f"❌ Failed batch ({resp.status_code}) → {resp.text}")

def send_in_batches(all_events: list[dict], batch_size: int = BATCH_SIZE) -> None:
    batch = []
    for ev in all_events:
        batch.append(ev)
        if len(batch) >= batch_size:
            send_batch(batch)
            batch = []
    if batch:
        send_batch(batch)

# =========================
# Main
# =========================
def main():
    print("🔄 Starting RICS → Meta Offline Conversions sync...")
    if not ACCESS_TOKEN or DATASET_ID in (None, "None", "",):
        print("❌ Missing META_ACCESS_TOKEN or META_DATASET_ID")
        return

    if not os.path.exists(INPUT_CSV_PATH):
        print(f"❌ CSV not found at {INPUT_CSV_PATH}")
        return

    events = build_events_from_csv(INPUT_CSV_PATH)
    if not events:
        print("ℹ️ No eligible events to send.")
        return

    print(f"📦 Prepared {len(events)} offline Purchase events")
    send_in_batches(events, BATCH_SIZE)
    print("✅ Finished sending events.")

if __name__ == "__main__":
    main()
