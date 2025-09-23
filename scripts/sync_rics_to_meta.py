import csv
import os
import time
import hashlib
import requests
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta

# =========================
# Config
# =========================
DATASET_ID = os.getenv("META_DATASET_ID")
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
API_URL = f"https://graph.facebook.com/v19.0/{DATASET_ID}/events"
HEADERS = {"Content-Type": "application/json"}

INPUT_CSV_PATH = os.getenv("RICS_INPUT_CSV", "rics_customer_purchase_history_deduped.csv")
BATCH_SIZE = 100
CURRENCY = "USD"
COUNTRY_DEFAULT = "US"

# Meta requires events <= 7 days old
MAX_EVENT_AGE_DAYS = 7
cutoff_time = datetime.utcnow() - timedelta(days=MAX_EVENT_AGE_DAYS)

# =========================
# Helpers
# =========================
def sha256_norm(value: str) -> str | None:
    if not value:
        return None
    v = value.strip().lower()
    if not v:
        return None
    return hashlib.sha256(v.encode("utf-8")).hexdigest()

def to_e164(phone: str) -> str | None:
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

def to_epoch(dt_string: str) -> int | None:
    if not dt_string:
        return None
    fmts = (
        "%Y-%m-%dT%H:%M:%S",      # ISO format: 2025-09-21T01:25:39
        "%Y-%m-%dT%H:%M:%S.%f",   # ISO with microseconds: 2025-09-21T01:25:39.123456
        "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO with microseconds and Z: 2025-09-21T01:25:39.123456Z
        "%Y-%m-%dT%H:%M:%SZ",     # ISO with Z: 2025-09-21T01:25:39Z
        "%Y-%m-%d %H:%M:%S",      # Space format: 2025-09-21 01:25:39
        "%m/%d/%Y %H:%M:%S",      # US format: 09/21/2025 01:25:39
        "%m/%d/%Y %H:%M"          # US format without seconds: 09/21/2025 01:25
    )
    for fmt in fmts:
        try:
            dt = datetime.strptime(dt_string.strip(), fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            continue
    return None

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

def booly(v) -> bool:
    return str(v).strip().lower() in {"y", "yes", "true", "1"}

# =========================
# Build Events
# =========================
def build_events_from_csv(csv_path: str) -> list[dict]:
    tickets = defaultdict(list)

    with open(csv_path, mode="r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if booly(row.get("TicketVoided")) or booly(row.get("TicketSuspended")):
                continue
            ticket_no = (row.get("TicketNumber") or "").strip()
            if not ticket_no:
                continue
            tickets[ticket_no].append(row)

    events = []
    skip_reasons = {"old": 0, "zero_value": 0, "flags": 0, "missing_keys": 0}

    for ticket_no, lines in tickets.items():
        first = lines[0]

        # Parse event_time
        sale_dt_str = first.get("SaleDateTime") or first.get("TicketDateTime")
        event_time = to_epoch(sale_dt_str)
        if not event_time:
            print(f"⚠️ Skipping event - no valid timestamp: {sale_dt_str}")
            continue
        event_dt = datetime.fromtimestamp(event_time, tz=timezone.utc)
        
        # Debug: Log first few events and their dates
        if len(events) < 3:
            print(f"🔍 DEBUG: Event date: {event_dt}, Cutoff: {cutoff_time.replace(tzinfo=timezone.utc)}")
            print(f"🔍 DEBUG: Event is {'OLD' if event_dt < cutoff_time.replace(tzinfo=timezone.utc) else 'RECENT'}")
        
        if event_dt < cutoff_time.replace(tzinfo=timezone.utc):
            skip_reasons["old"] += 1
            continue

        # Calculate total value & contents
        total_value = 0.0
        contents = []
        for ln in lines:
            qty = safe_float(ln.get("Quantity"))
            amt = safe_float(ln.get("AmountPaid"))
            if amt > 0:
                total_value += amt
            unit_price = amt / qty if qty > 0 else amt
            contents.append({
                "id": (ln.get("Sku") or "UNKNOWN").strip(),
                "quantity": safe_int(qty) if qty > 0 else 1,
                "item_price": round(unit_price, 2)
            })
        if total_value <= 0:
            skip_reasons["zero_value"] += 1
            continue

        if booly(first.get("TicketVoided")) or booly(first.get("TicketSuspended")):
            skip_reasons["flags"] += 1
            continue

        # Match keys - using actual CSV field names
        customer_name = first.get("CustomerName", "").strip()
        name_parts = customer_name.split(" ", 1) if customer_name else ["", ""]
        
        # Ensure we have at least 2 parts (first name, last name)
        if len(name_parts) < 2:
            name_parts.extend([""] * (2 - len(name_parts)))
        
        mk = {
            "em": sha256_norm(first.get("CustomerEmail")),
            "ph": sha256_phone(first.get("CustomerPhone")),
            "fn": sha256_norm(name_parts[0]),  # First name
            "ln": sha256_norm(name_parts[1]),  # Last name
            "ct": sha256_norm(first.get("City")),
            "st": sha256_norm(first.get("State")),
            "zip": sha256_norm(str(first.get("ZipCode") or "")),
            "country": sha256_norm(COUNTRY_DEFAULT),
            "external_id": sha256_norm(first.get("CustomerId")),
        }
        match_keys = {k: v for k, v in mk.items() if v}
        if not match_keys:
            skip_reasons["missing_keys"] += 1
            continue

        event = {
            "event_name": "Purchase",
            "event_time": event_time,
            "event_id": f"purchase-{ticket_no}",
            "event_source_url": "https://prrc-connector.com",
            "match_keys": match_keys,
            "custom_data": {
                "order_id": str(ticket_no),
                "value": round(total_value, 2),
                "currency": CURRENCY,
                "contents": contents,
            },
        }
        events.append(event)

    print(f"ℹ️ Skip summary: {skip_reasons}")
    print(f"🔍 DEBUG: Total events processed: {len(tickets)}")
    print(f"🔍 DEBUG: Events after filtering: {len(events)}")
    return events

# =========================
# Sender
# =========================
def send_batch(events: list[dict]) -> None:
    payload = {"data": events, "access_token": ACCESS_TOKEN}
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
    import sys
    
    # Use command line argument if provided, otherwise use environment variable
    csv_path = sys.argv[1] if len(sys.argv) > 1 else INPUT_CSV_PATH
    
    print("🔄 Starting RICS → Meta Offline Conversions sync...")
    if not ACCESS_TOKEN or not DATASET_ID:
        print("❌ Missing META_ACCESS_TOKEN or META_DATASET_ID")
        return
    if not os.path.exists(csv_path):
        print(f"❌ CSV not found at {csv_path}")
        return

    events = build_events_from_csv(csv_path)
    if not events:
        print("ℹ️ No eligible events to send.")
        return

    print(f"📦 Prepared {len(events)} offline Purchase events")
    send_in_batches(events, BATCH_SIZE)
    print("✅ Finished sending events.")

if __name__ == "__main__":
    main()
