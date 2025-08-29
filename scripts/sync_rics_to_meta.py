import csv
import os
import time
import hashlib
import requests
import re
from collections import defaultdict
from datetime import datetime, timezone

# =========================
# Config
# =========================
DATASET_ID = os.getenv("META_DATASET_ID")  # <- set this secret in Actions
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
API_URL = f"https://graph.facebook.com/v19.0/{DATASET_ID}/events"
HEADERS = {"Content-Type": "application/json"}

INPUT_CSV_PATH = os.getenv(
    "RICS_INPUT_CSV",
    "optimizely_connector/output/rics_customer_purchase_history_latest.csv"
)

BATCH_SIZE = 100
CURRENCY = "USD"
COUNTRY_DEFAULT = "US"

# =========================
# Helpers
# =========================
def sha256_norm(value: str):
    if not value:
        return None
    v = value.strip().lower()
    if not v:
        return None
    return hashlib.sha256(v.encode("utf-8")).hexdigest()

def to_e164(phone: str):
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 10:  # US default
        digits = "1" + digits
    if len(digits) < 11 or len(digits) > 15:
        return None
    return "+" + digits

def sha256_phone(phone: str):
    e164 = to_e164(phone)
    return hashlib.sha256(e164.encode("utf-8")).hexdigest() if e164 else None

def to_epoch(dt_string: str) -> int:
    if not dt_string:
        return int(time.time())
    fmts = ("%Y-%m-%d %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M")
    for fmt in fmts:
        try:
            dt = datetime.strptime(dt_string.strip(), fmt)
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        except Exception:
            continue
    return int(time.time())

def booly(v) -> bool:
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
# Build events grouped by TicketNumber
# =========================
def build_events_from_csv(csv_path: str):
    tickets = defaultdict(list)

    with open(csv_path, mode="r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if booly(row.get("TicketVoidedPrinted")) or booly(row.get("TicketVoided")):
                continue
            if booly(row.get("TicketSuspended")):
                continue

            ticket_no = (row.get("TicketNumber") or "").strip()
            if not ticket_no:
                continue
            tickets[ticket_no].append(row)

    events = []
    for ticket_no, lines in tickets.items():
        first = lines[0]
        event_time = to_epoch(first.get("SaleDateTime") or first.get("TicketDateTime"))

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
            continue

        # user_data (hashed identifiers) for Offline Event Sets
        user_data = {
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
        # drop empties
        user_data = {k: v for k, v in user_data.items() if v}

        event = {
            "event_name": "Purchase",
            "event_time": event_time,
            "event_id": f"purchase-{ticket_no}",  # dedup across retries
            "action_source": "offline",
            "user_data": user_data,
            "custom_data": {
                "order_id": str(ticket_no),
                "value": round(total_value, 2),
                "currency": CURRENCY,
                "contents": contents
            }
        }
        events.append(event)

    return events

# =========================
# Sender
# =========================
def send_batch(events):
    payload = {"data": events, "access_token": ACCESS_TOKEN}
    resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=60)
    if resp.ok:
        print(f"✅ Sent batch of {len(events)} events")
    else:
        print(f"❌ Failed batch ({resp.status_code}) → {resp.text}")
        # surface non-200 as failure for CI visibility
        resp.raise_for_status()

def send_in_batches(all_events, size=BATCH_SIZE):
    batch = []
    for ev in all_events:
        batch.append(ev)
        if len(batch) >= size:
            send_batch(batch)
            batch = []
    if batch:
        send_batch(batch)

# =========================
# Main
# =========================
def main():
    print("🔄 Starting RICS → Meta Offline Conversions sync...")
    if not ACCESS_TOKEN or not DATASET_ID:
        print("❌ Missing META_ACCESS_TOKEN or META_DATASET_ID")
        raise SystemExit(1)
    if not os.path.exists(INPUT_CSV_PATH):
        print(f"❌ CSV not found at {INPUT_CSV_PATH}")
        raise SystemExit(1)

    events = build_events_from_csv(INPUT_CSV_PATH)
    if not events:
        print("ℹ️ No eligible events to send.")
        return

    print(f"📦 Prepared {len(events)} offline Purchase events")
    send_in_batches(events, BATCH_SIZE)
    print("✅ Finished sending events.")

if __name__ == "__main__":
    main()
