import os
import csv
import hashlib
import time
import requests

# Config from env
OFFLINE_SET_ID = os.environ["META_OFFLINE_SET_ID"]
ACCESS_TOKEN   = os.environ["META_OFFLINE_TOKEN"]
RICS_DATA_PATH = os.environ.get("RICS_CSV_PATH", "./data/rics.csv")

def sha256(s):
    return hashlib.sha256(s.strip().lower().encode("utf-8")).hexdigest()

def load_rics_events(csv_path):
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield {
                "event_name": "Purchase",
                "event_time": int(time.mktime(time.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%S"))),
                "event_id": row["order_id"],
                "user_data": {
                    "em": sha256(row["email"]),
                    "ph": sha256(row["phone"]),
                    # add more hashes if you like...
                },
                "custom_data": {
                    "value": float(row["total_amount"]),
                    "currency": "USD"
                }
            }

def push_to_meta(events):
    url = f"https://graph.facebook.com/v16.0/{OFFLINE_SET_ID}/events"
    payload = {"data": events}
    params  = {"access_token": ACCESS_TOKEN}
    resp = requests.post(url, json=payload, params=params)
    resp.raise_for_status()
    print("Success:", resp.json())

if __name__ == "__main__":
    batch = list(load_rics_events(RICS_DATA_PATH))[:50]  # slice small for test
    push_to_meta(batch)
