import csv
import os
import time
import json
import hashlib
import requests
from datetime import datetime, timezone

# Constants
DATASET_ID = os.getenv("META_DATASET_ID", "855183627077424")
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
API_URL = f"https://graph.facebook.com/v19.0/{DATASET_ID}/events"
HEADERS = {"Content-Type": "application/json"}

INPUT_CSV_PATH = "optimizely_connector/output/rics_customer_purchase_history_latest.csv"

# Helper Functions
def hash_data(value):
    return hashlib.sha256(value.strip().lower().encode("utf-8")).hexdigest() if value else None

def get_unix_timestamp(dt_string):
    try:
        dt = datetime.strptime(dt_string, "%Y-%m-%d %H:%M:%S")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except Exception as e:
        print(f"❌ Error parsing datetime '{dt_string}': {e}")
        return int(time.time())

def build_event(customer):
    user_data = {
        "em": hash_data(customer.get("email")),
        "ph": hash_data(customer.get("phone")),
        "fn": hash_data(customer.get("first_name")),
        "ln": hash_data(customer.get("last_name")),
        "ct": hash_data(customer.get("city")),
        "st": hash_data(customer.get("state")),
        "zp": hash_data(str(customer.get("zip"))),
    }

    user_data = {k: v for k, v in user_data.items() if v}

    return {
        "match_keys": user_data,
        "event_name": "Purchase",
        "event_time": get_unix_timestamp(customer.get("SaleDateTime")),
        "action_source": "physical_store",
        "custom_data": {
            "value": float(customer.get("AmountPaid") or 0),
            "currency": "USD"
        }
    }

def send_batch(events):
    payload = {
        "data": events,
        "access_token": ACCESS_TOKEN
    }
    response = requests.post(API_URL, headers=HEADERS, json=payload)
    if response.ok:
        print(f"✅ Sent batch of {len(events)} events successfully")
    else:
        print(f"❌ Failed to send batch: {response.status_code}")
        print(response.text)

def main():
    print("🔄 Starting RICS to Meta sync...")

    if not ACCESS_TOKEN or DATASET_ID == "None":
        print("❌ Missing META_ACCESS_TOKEN or META_DATASET_ID")
        return

    events = []
    with open(INPUT_CSV_PATH, mode="r", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        row_num = 0
        for row in reader:
            row_num += 1
            if not row.get("email") and not row.get("phone"):
                continue  # Skip contacts with no matchable ID

            event = build_event(row)
            events.append(event)

            if len(events) >= 100:  # Send in batches of 100
                send_batch(events)
                events = []

        # Send final batch
        if events:
            send_batch(events)

    print(f"\n✅ Finished sending events from {row_num} CSV rows")

if __name__ == "__main__":
    main()
