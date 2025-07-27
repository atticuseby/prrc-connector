import csv
import os
import sys
from datetime import datetime
from dateutil.parser import parse as parse_date
import hashlib
import json
import requests

print("🔄 Starting RICS to Meta sync...")

CSV_PATH = "optimizely_connector/output/rics_customer_purchase_history_latest.csv"
ACCESS_TOKEN = os.getenv("META_ACCESS_TOKEN")
PIXEL_ID = os.getenv("META_PIXEL_ID")
API_VERSION = "v19.0"
EVENT_NAME = "Purchase"
EVENT_SOURCE_URL = "https://prrunandwalk.com"
UPLOAD_TAG = "RICS-Offline-Sync"
TEST_EVENT_CODE = os.getenv("META_TEST_CODE")  # Optional
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

if not os.path.exists(CSV_PATH):
    print(f"❌ ERROR: CSV not found at {CSV_PATH}")
    sys.exit(1)

events = []

with open(CSV_PATH, newline="", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)
    for row_num, row in enumerate(reader, start=1):
        email = row.get("email", "").strip().lower()
        phone = row.get("phone", "").strip()
        amount_paid = float(row.get("AmountPaid", "0") or "0")

        # Skip if no email or phone
        if not email and not phone:
            print(f"⛔ SKIP row {row_num}: missing email & phone", end=" | ")
            reason_logged = True
        else:
            reason_logged = False

        # Skip if no revenue
        if amount_paid == 0:
            print(f"⛔ SKIP row {row_num}: AmountPaid = 0", end=" | ")
            reason_logged = True

        # Parse and validate timestamp
        ticket_datetime_raw = row.get("TicketDateTime", "").strip()
        try:
            ticket_datetime = parse_date(ticket_datetime_raw)
            timestamp = int(ticket_datetime.timestamp())
        except Exception:
            print(f"⛔ SKIP row {row_num}: invalid TicketDateTime format", end=" | ")
            reason_logged = True
            timestamp = None

        if reason_logged:
            print()
            continue

        user_data = {}
        if email:
            user_data["em"] = [hashlib.sha256(email.encode()).hexdigest()]
        if phone:
            cleaned_phone = ''.join(filter(str.isdigit, phone))
            if len(cleaned_phone) >= 10:
                user_data["ph"] = [hashlib.sha256(cleaned_phone.encode()).hexdigest()]

        event = {
            "event_name": EVENT_NAME,
            "event_time": timestamp,
            "event_source_url": EVENT_SOURCE_URL,
            "user_data": user_data,
            "custom_data": {
                "value": round(amount_paid, 2),
                "currency": "USD",
            }
        }

        if TEST_EVENT_CODE:
            event["test_event_code"] = TEST_EVENT_CODE

        events.append(event)

print(f"\n✅ Loaded {len(events)} valid events from {row_num} CSV rows")

if DRY_RUN or not events:
    print("🛑 DRY RUN or no valid events. Aborting before API call.")
    sys.exit(0)

# Push to Meta
url = f"https://graph.facebook.com/{API_VERSION}/{PIXEL_ID}/events"
payload = {
    "data": events,
    "access_token": ACCESS_TOKEN,
    "upload_tag": UPLOAD_TAG,
}

response = requests.post(url, json=payload)
if response.ok:
    print(f"✅ Upload complete. Response: {response.json()}")
else:
    print(f"❌ Upload failed. Status: {response.status_code} | Response: {response.text}")
    sys.exit(1)
