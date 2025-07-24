import os
import csv
import hashlib
import time
import requests
import sys
import json
from datetime import datetime

# Config from env
META_OFFLINE_SET_ID = os.environ.get("META_OFFLINE_SET_ID")
META_OFFLINE_TOKEN = os.environ.get("META_OFFLINE_TOKEN")
RICS_DATA_PATH = os.environ.get("RICS_CSV_PATH", "./data/rics.csv")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))

def validate_environment():
    """Validate required environment variables"""
    missing_vars = []
    if not META_OFFLINE_SET_ID:
        missing_vars.append("META_OFFLINE_SET_ID")
    if not META_OFFLINE_TOKEN:
        missing_vars.append("META_OFFLINE_TOKEN")
    
    if missing_vars:
        print(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        print("Please set these in your GitHub repository secrets or environment")
        sys.exit(1)
    
    print(f"✅ Environment validation passed")
    print(f"   Offline Set ID: {META_OFFLINE_SET_ID}")
    print(f"   Token present: {'✅' if META_OFFLINE_TOKEN else '❌'}")

def sha256(s):
    """Create SHA256 hash of string, handling empty values"""
    if not s or not s.strip():
        return ""
    return hashlib.sha256(s.strip().lower().encode("utf-8")).hexdigest()

def validate_csv_format(csv_path):
    """Validate CSV file exists and has required columns"""
    if not os.path.exists(csv_path):
        print(f"❌ CSV file not found: {csv_path}")
        return False
    
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            print(f"📋 CSV headers found: {headers}")
            
            # Check for required fields
            required_fields = ["email", "phone", "first_name", "last_name"]
            missing_fields = [field for field in required_fields if headers and field not in headers]
            
            if missing_fields:
                print(f"⚠️ Missing fields in CSV: {missing_fields}")
                print("Will use empty values for missing fields")
            
            return True
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return False

def load_rics_events(csv_path):
    """Load and validate RICS events from CSV"""
    events = []
    row_count = 0

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1

            # Validate required data
            email = row.get("email", "").strip()
            phone = row.get("phone", "").strip()

            if not email and not phone:
                print(f"⚠️ Skipping row {row_count}: no email or phone")
                continue

            # Safely parse AmountPaid
            try:
                amount_paid = float(row.get("AmountPaid", 0))
            except (ValueError, TypeError):
                print(f"⚠️ Skipping row {row_count}: invalid AmountPaid")
                continue

            if amount_paid <= 0:
                print(f"⚠️ Skipping row {row_count}: AmountPaid is zero or missing")
                continue

            # Safely parse recent TicketDateTime
            event_time = int(time.time())  # fallback to now
            if "TicketDateTime" in row and row["TicketDateTime"]:
                try:
                    timestamp_str = row["TicketDateTime"].strip()
                    for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
                        try:
                            parsed_time = int(time.mktime(time.strptime(timestamp_str, fmt)))
                            if parsed_time > int(time.time()) - 7 * 86400:
                                event_time = parsed_time
                            else:
                                print(f"⚠️ Skipping row {row_count}: event too old")
                                raise ValueError
                            break
                        except ValueError:
                            continue
                except Exception:
                    continue

            # Create event
            event = {
                "event_name": "Purchase",
                "event_time": event_time,
                "event_id": str(row.get("rics_id", f"rics_{row_count}")),
                "user_data": {
                    "em": sha256(email),
                    "ph": sha256(phone),
                    "fn": sha256(row.get("first_name", "")),
                    "ln": sha256(row.get("last_name", "")),
                },
                "custom_data": {
                    "currency": "USD"
                }
            }
            
            # Add value only if valid
            if isinstance(amount_paid, (int, float)) and amount_paid > 0:
                event["custom_data"]["value"] = round(amount_paid, 2)
            else:
                print(f"⚠️ Row {row_count}: invalid amount_paid = {amount_paid} — skipping")
                continue
            
            # Remove empty hashed user_data fields
            event["user_data"] = {k: v for k, v in event["user_data"].items() if v}

            events.append(event)

            if row_count <= 3:
                print(f"📝 Sample event {row_count}:")
                print(f"   Email: {email[:20]}{'...' if len(email) > 20 else ''}")
                print(f"   Phone: {phone[:10]}{'...' if len(phone) > 10 else ''}")
                print(f"   Event time: {datetime.fromtimestamp(event_time)}")

    print(f"✅ Loaded {len(events)} valid events from {row_count} CSV rows")
    return events

def test_meta_connection():
    """Test Meta API connection and permissions"""
    print("🔍 Testing Meta API connection...")
    
    # Test basic API access
    try:
        url = f"https://graph.facebook.com/v16.0/{META_OFFLINE_SET_ID}"
        params = {"access_token": META_OFFLINE_TOKEN}
        resp = requests.get(url, params=params, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"✅ Offline Event Set found: {data.get('name', 'Unknown')}")
            return True
        else:
            print(f"❌ Failed to access Offline Event Set: {resp.status_code}")
            print(f"   Response: {resp.text}")
            return False
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False

def push_to_meta(events):
    """Push events to Meta with detailed error handling"""
    url = f"https://graph.facebook.com/v16.0/{META_OFFLINE_SET_ID}/events"
    payload = {"data": events}
    params = {"access_token": META_OFFLINE_TOKEN}
    
    print(f"📤 Sending {len(events)} events to Meta...")
    
    try:
        resp = requests.post(url, json=payload, params=params, timeout=30)
        
        if resp.status_code == 200:
            result = resp.json()
            print(f"✅ Success: {result.get('events_received', 0)} events received")
            return True
        else:
            print(f"❌ Meta API Error: {resp.status_code}")
            print(f"   URL: {url}")
            print(f"   Response: {resp.text}")
            
            # Try to parse error details
            try:
                error_data = resp.json()
                if "error" in error_data:
                    error = error_data["error"]
                    print(f"   Error Type: {error.get('type', 'Unknown')}")
                    print(f"   Error Code: {error.get('code', 'Unknown')}")
                    print(f"   Error Message: {error.get('message', 'No message')}")
                    
                    # Common error codes and solutions
                    if error.get('code') == 100:
                        print("   💡 Solution: Check your access token permissions")
                    elif error.get('code') == 190:
                        print("   💡 Solution: Invalid access token - generate a new one")
                    elif error.get('code') == 294:
                        print("   💡 Solution: Check offline event set ID")
            except:
                pass
            
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Request timeout - Meta API is slow")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        return False

if __name__ == "__main__":
    try:
        print("🔄 Starting RICS to Meta sync...")
        print(f"   CSV path: {RICS_DATA_PATH}")
        print(f"   Batch size: {BATCH_SIZE}")
        print()
        
        # Validate environment
        validate_environment()
        
        # Test Meta connection
        if not test_meta_connection():
            print("❌ Meta API connection failed - check credentials")
            sys.exit(1)
        
        # Validate CSV
        if not validate_csv_format(RICS_DATA_PATH):
            print("❌ CSV validation failed")
            sys.exit(1)
        
        # Load events
        all_events = load_rics_events(RICS_DATA_PATH)
        
        if not all_events:
            print("❌ No valid events found in CSV")
            sys.exit(1)
        
        # Process in batches
        total_events = len(all_events)
        processed = 0
        failed_batches = 0
        
        for i in range(0, total_events, BATCH_SIZE):
            batch = all_events[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_events + BATCH_SIZE - 1) // BATCH_SIZE
            
            print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} events)")
            
            if push_to_meta(batch):
                processed += len(batch)
            else:
                failed_batches += 1
                print(f"⚠️ Batch {batch_num} failed - continuing with next batch")
        
        print(f"\n📊 Sync Summary:")
        print(f"   Total events: {total_events}")
        print(f"   Processed: {processed}")
        print(f"   Failed batches: {failed_batches}")
        
        if failed_batches == 0:
            print("🎉 All events processed successfully!")
        else:
            print(f"⚠️ {failed_batches} batches failed - check logs above")
            sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Sync failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
