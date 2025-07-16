import os
import csv
import hashlib
import time
import requests
import sys

# Config from env
META_OFFLINE_SET_ID = os.environ["META_OFFLINE_SET_ID"]
META_OFFLINE_TOKEN = os.environ["META_OFFLINE_TOKEN"]
RICS_DATA_PATH = os.environ.get("RICS_CSV_PATH", "./data/rics.csv")
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE", "50"))  # Make configurable

def sha256(s):
    return hashlib.sha256(s.strip().lower().encode("utf-8")).hexdigest() if s else ""

def load_rics_events(csv_path):
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Use timestamp from CSV if present, else use current time
            if "timestamp" in row and row["timestamp"]:
                try:
                    event_time = int(time.mktime(time.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%S")))
                except Exception:
                    event_time = int(time.time())
            else:
                event_time = int(time.time())
            yield {
                "event_name": "Purchase",
                "event_time": event_time,
                "event_id": row.get("rics_id", ""),
                "user_data": {
                    "em": sha256(row.get("email", "")),
                    "ph": sha256(row.get("phone", "")),
                    "fn": sha256(row.get("first_name", "")),
                    "ln": sha256(row.get("last_name", "")),
                },
                "custom_data": {
                    "value": float(row["total_spent"]) if row.get("total_spent") else 0.0,
                    "currency": "USD"
                }
            }

def push_to_meta(events):
    url = f"https://graph.facebook.com/v16.0/{META_OFFLINE_SET_ID}/events"
    payload = {"data": events}
    params = {"access_token": META_OFFLINE_TOKEN}
    resp = requests.post(url, json=payload, params=params)
    resp.raise_for_status()
    print("Success:", resp.json())

if __name__ == "__main__":
    try:
        print("🔄 Starting RICS to Meta sync...")
        print(f"   CSV path: {RICS_DATA_PATH}")
        print(f"   Batch size: {BATCH_SIZE}")
        print()
        
        # Load events
        all_events = list(load_rics_events(RICS_DATA_PATH))
        
        if not all_events:
            print("❌ No valid events found in CSV")
            sys.exit(1)
        
        # Process in batches
        total_events = len(all_events)
        processed = 0
        
        for i in range(0, total_events, BATCH_SIZE):
            batch = all_events[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_events + BATCH_SIZE - 1) // BATCH_SIZE
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} events)")
            push_to_meta(batch)
            processed += len(batch)
        
        print(f"\n🎉 Successfully processed {processed} events!")
        
    except Exception as e:
        print(f"\n❌ Sync failed: {e}")
        sys.exit(1)
