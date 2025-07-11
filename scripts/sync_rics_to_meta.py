import os
import csv
import hashlib
import time
import requests
import sys

# Config from env
OFFLINE_SET_ID = os.environ["META_OFFLINE_SET_ID"]
ACCESS_TOKEN   = os.environ["META_OFFLINE_TOKEN"]
RICS_DATA_PATH = os.environ.get("RICS_CSV_PATH", "./data/rics.csv")
BATCH_SIZE     = int(os.environ.get("BATCH_SIZE", "50"))  # Make configurable

def sha256(s):
    return hashlib.sha256(s.strip().lower().encode("utf-8")).hexdigest()

def load_rics_events(csv_path):
    """Load RICS events from CSV and convert to Meta format"""
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    events = []
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        
        # Check required fields
        required_fields = ["timestamp", "order_id", "email", "phone", "total_amount"]
        missing_fields = [field for field in required_fields if field not in reader.fieldnames]
        if missing_fields:
            raise ValueError(f"Missing required CSV fields: {missing_fields}")
        
        for row_num, row in enumerate(reader, start=2):  # Start at 2 because row 1 is header
            try:
                # Validate required data
                if not all(row.get(field) for field in required_fields):
                    print(f"⚠️  Skipping row {row_num}: Missing required data")
                    continue
                
                # Parse timestamp
                try:
                    event_time = int(time.mktime(time.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%S")))
                except ValueError as e:
                    print(f"⚠️  Skipping row {row_num}: Invalid timestamp '{row['timestamp']}': {e}")
                    continue
                
                # Parse total amount
                try:
                    value = float(row["total_amount"])
                except ValueError as e:
                    print(f"⚠️  Skipping row {row_num}: Invalid total_amount '{row['total_amount']}': {e}")
                    continue
                
                event = {
                    "event_name": "Purchase",
                    "event_time": event_time,
                    "event_id": row["order_id"],
                    "user_data": {
                        "em": sha256(row["email"]),
                        "ph": sha256(row["phone"]),
                    },
                    "custom_data": {
                        "value": value,
                        "currency": "USD"
                    }
                }
                events.append(event)
                
            except Exception as e:
                print(f"⚠️  Error processing row {row_num}: {e}")
                continue
    
    print(f"📊 Loaded {len(events)} valid events from CSV")
    return events

def push_to_meta(events):
    """Push events to Meta Offline Events API"""
    
    if not events:
        print("⚠️  No events to send")
        return
    
    url = f"https://graph.facebook.com/v16.0/{OFFLINE_SET_ID}/events"
    payload = {"data": events}
    params = {"access_token": ACCESS_TOKEN}
    
    print(f"📤 Sending {len(events)} events to Meta...")
    
    try:
        resp = requests.post(url, json=payload, params=params, timeout=30)
        resp.raise_for_status()
        
        result = resp.json()
        print("✅ Successfully sent to Meta!")
        print(f"   Response: {result}")
        
        # Check for any errors in the response
        if "events_received" in result:
            print(f"   Events received: {result['events_received']}")
        if "messages" in result:
            for msg in result["messages"]:
                if msg.get("error"):
                    print(f"   ⚠️  Error: {msg['error']}")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to send to Meta: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response status: {e.response.status_code}")
            print(f"   Response body: {e.response.text}")
        raise

if __name__ == "__main__":
    try:
        print("🔄 Starting RICS to Meta sync...")
        print(f"   CSV path: {RICS_DATA_PATH}")
        print(f"   Batch size: {BATCH_SIZE}")
        print()
        
        # Load events
        all_events = load_rics_events(RICS_DATA_PATH)
        
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
