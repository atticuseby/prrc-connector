import os
import csv
import hashlib
import time
import requests
import sys
import json
from datetime import datetime

# Config from env
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN")
META_OFFLINE_EVENT_SET_ID = os.environ.get("META_OFFLINE_EVENT_SET_ID")
RICS_DATA_PATH = os.environ.get("RICS_CSV_PATH", "./data/rics.csv")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

# CHANGE THIS if you want to extend how far back events are allowed (for testing)
EVENT_AGE_LIMIT_SECONDS = 7 * 86400  # 7 days

def validate_environment():
    missing_vars = []
    if not META_ACCESS_TOKEN:
        missing_vars.append("META_ACCESS_TOKEN")
    if not META_OFFLINE_EVENT_SET_ID:
        missing_vars.append("META_OFFLINE_EVENT_SET_ID")
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)
    
    print(f"✅ Environment validation passed")
    print(f"   Offline Set ID: {META_OFFLINE_EVENT_SET_ID}")
    print(f"   Token present: ✅")

def sha256(s):
    if not s or not s.strip():
        return ""
    return hashlib.sha256(s.strip().lower().encode("utf-8")).hexdigest()

def validate_csv_format(csv_path):
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return False
    
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            print(f"📋 CSV headers found: {headers}")
            return True
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return False

def load_rics_events(csv_path):
    events = []
    row_count = 0
    now_ts = int(time.time())

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            reasons = []

            email = row.get("email", "").strip()
            phone = row.get("phone", "").strip()
            first = row.get("first_name", "").strip()
            last = row.get("last_name", "").strip()
            amount_paid_raw = row.get("AmountPaid", "").strip()
            ticket_time_raw = row.get("TicketDateTime", "").strip()

            if not email and not phone:
                reasons.append("missing email & phone")

            if row.get("TicketVoided") == 'TRUE':
                reasons.append("TicketVoided = TRUE")
            if row.get("TicketSuspended") == 'TRUE':
                reasons.append("TicketSuspended = TRUE")

            # Parse ticket time
            event_time = now_ts
            too_old = False
            if ticket_time_raw:
                parsed = None
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                    try:
                        parsed = time.strptime(ticket_time_raw, fmt)
                        break
                    except:
                        continue
                if parsed:
                    event_time = int(time.mktime(parsed))
                    if event_time < now_ts - EVENT_AGE_LIMIT_SECONDS:
                        reasons.append("event too old (>7 days)")
                    elif event_time > now_ts + 60:
                        reasons.append("event in future")
                else:
                    reasons.append("invalid TicketDateTime format")
            else:
                reasons.append("missing TicketDateTime")

            try:
                amount_paid = float(amount_paid_raw)
                if amount_paid <= 0:
                    reasons.append("AmountPaid = 0")
            except:
                reasons.append("AmountPaid not a number")

            if reasons:
                print(f"⛔ Skipping row {row_count}: {' | '.join(reasons)}")
                continue

            event = {
                "event_name": "Purchase",
                "event_time": event_time,
                "event_id": str(row.get("rics_id", f"rics_{row_count}")),
                "action_source": "physical_store",
                "user_data": {
                    "em": sha256(email),
                    "ph": sha256(phone),
                    "fn": sha256(first),
                    "ln": sha256(last),
                },
                "custom_data": {
                    "currency": "USD",
                    "value": round(float(amount_paid), 2)
                }
            }

            # Remove empty fields
            event["user_data"] = {k: v for k, v in event["user_data"].items() if v}

            if row_count <= 3:
                print(f"📝 Sample event {row_count}: {json.dumps(event, indent=2)}")

            events.append(event)

    print(f"\n✅ Loaded {len(events)} valid events from {row_count} CSV rows")
    return events

def test_meta_connection():
    print("🔍 Testing Meta API connection...")
    try:
        url = f"https://graph.facebook.com/v16.0/{META_OFFLINE_EVENT_SET_ID}"
        params = {"access_token": META_ACCESS_TOKEN}
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            print(f"✅ Offline Event Set found: {data.get('name', 'Unknown')}")
            return True
        else:
            print(f"❌ Meta API error: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def push_to_meta(events):
    url = f"https://graph.facebook.com/v16.0/{META_OFFLINE_EVENT_SET_ID}/events"
    payload = {"data": events}
    params = {"access_token": META_ACCESS_TOKEN}

    print(f"📤 Sending {len(events)} events to Meta...")
    try:
        resp = requests.post(url, json=payload, params=params, timeout=30)
        if resp.status_code == 200:
            print(f"✅ Meta received {resp.json().get('events_received', 0)} events")
            return True
        else:
            print(f"❌ Meta error: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        print(f"❌ Network error: {e}")
        return False

if __name__ == "__main__":
    print("🔄 Starting RICS to Meta sync...")
    print(f"   CSV path: {RICS_DATA_PATH}")
    print(f"   Batch size: {BATCH_SIZE}")
    print()

    validate_environment()

    if not test_meta_connection():
        sys.exit(1)

    if not validate_csv_format(RICS_DATA_PATH):
        sys.exit(1)

    events = load_rics_events(RICS_DATA_PATH)

    if not events:
        print("❌ No valid events found in CSV")
        sys.exit(1)

    total = len(events)
    failed = 0
    sent = 0

    for i in range(0, total, BATCH_SIZE):
        batch = events[i:i+BATCH_SIZE]
        print(f"\n📦 Sending batch {i//BATCH_SIZE + 1}")
        if push_to_meta(batch):
            sent += len(batch)
        else:
            failed += 1

    print("\n📊 Sync Complete:")
    print(f"   Sent: {sent} / {total}")
    print(f"   Failed Batches: {failed}")
    if failed == 0:
        print("🎉 All batches successful!")
    else:
        sys.exit(1)
