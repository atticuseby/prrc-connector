import csv
import hashlib
import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()

DATASET_ID = os.getenv("META_DATASET_ID")
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
TEST_EVENT_CODE = os.getenv("META_TEST_EVENT_CODE")
API_URL = f"https://graph.facebook.com/v19.0/{DATASET_ID}/events"

def hash_data(value):
    return hashlib.sha256(value.strip().lower().encode()).hexdigest()

def build_event(row):
    match_keys = {}

    if row.get("email"):
        match_keys["em"] = hash_data(row["email"])
    if row.get("phone"):
        match_keys["ph"] = hash_data(row["phone"])
    if row.get("first_name") and row.get("last_name") and row.get("zip"):
        match_keys.update({
            "fn": hash_data(row["first_name"]),
            "ln": hash_data(row["last_name"]),
            "zp": hash_data(row["zip"])
        })

    if not match_keys:
        return None

    return {
        "match_keys": match_keys,
        "event_name": "OfflinePurchase",
        "event_time": 1721935820,  # Fixed test timestamp
        "value": 99.99,
        "currency": "USD"
    }

def send_test_event():
    with open("data/test_event.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            event = build_event(row)
            if event:
                payload = {
                    "data": [event],
                    "test_event_code": TEST_EVENT_CODE,
                    "access_token": ACCESS_TOKEN
                }
                response = requests.post(API_URL, json=payload)
                print("📤 Sent test event. Response:")
                print(response.status_code, response.text)

if __name__ == "__main__":
    send_test_event()
