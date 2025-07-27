import os
import csv
import hashlib
import time
import requests
import sys
import json
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io
import re

# === CONFIG ===
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = 'optimizely_connector/service_account.json'
DRIVE_FOLDER_ID = os.environ.get("GDRIVE_FOLDER_ID_RICS")
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN")
META_OFFLINE_EVENT_SET_ID = os.environ.get("META_OFFLINE_EVENT_SET_ID")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "50"))
EVENT_AGE_LIMIT_SECONDS = 7 * 86400
TEMP_CSV_PATH = "/tmp/rics_cleaned_last24h.csv"

# === GOOGLE DRIVE ===
def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    return build("drive", "v3", credentials=creds)

def download_latest_csv():
    service = get_drive_service()
    results = service.files().list(
        q=f"'{DRIVE_FOLDER_ID}' in parents and mimeType='text/csv'",
        orderBy="modifiedTime desc",
        pageSize=1,
        fields="files(id, name, modifiedTime)"
    ).execute()
    items = results.get("files", [])
    if not items:
        print("❌ No CSV files found in Drive folder.")
        sys.exit(1)
    file = items[0]
    print(f"📥 Downloading: {file['name']} (Last Modified: {file['modifiedTime']})")
    request = service.files().get_media(fileId=file["id"])
    fh = io.FileIO(TEMP_CSV_PATH, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return TEMP_CSV_PATH

# === HELPERS ===
def sha256(s):
    if not s or not s.strip():
        return ""
    return hashlib.sha256(s.strip().lower().encode("utf-8")).hexdigest()

def clean_string(s):
    return re.sub(r"[^\x00-\x7F]+", "", s or "").strip()

# === LOAD RICS EVENTS ===
def load_rics_events(csv_path):
    events = []
    row_count = 0
    now_ts = int(time.time())

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_count += 1
            reasons = []

            email = clean_string(row.get("email"))
            phone = clean_string(row.get("phone"))
            first = clean_string(row.get("first_name"))
            last = clean_string(row.get("last_name"))
            amount_paid_raw = clean_string(row.get("AmountPaid"))
            ticket_time_raw = clean_string(row.get("TicketDateTime"))

            if not email and not phone:
                reasons.append("missing email & phone")
            if row.get("TicketVoided") == 'TRUE':
                reasons.append("TicketVoided = TRUE")
            if row.get("TicketSuspended") == 'TRUE':
                reasons.append("TicketSuspended = TRUE")

            # Parse TicketDateTime
            event_time = now_ts
            if ticket_time_raw:
                parsed = None
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
                    try:
                        parsed = time.strptime(ticket_time_raw, fmt)
                        break
                    except:
                        continue
                if parsed:
                    event_time = int(time.mktime(parsed))
                    if event_time < now_ts - EVENT_AGE_LIMIT_SECONDS:
                        reasons.append("event too old")
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
                print(f"⛔ SKIP row {row_count}: {' | '.join(reasons)}")
                continue

            event = {
                "event_name": "Purchase",
                "event_time": event_time,
                "event_id": str(row.get("rics_id", f"row{row_count}")),
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

            event["user_data"] = {k: v for k, v in event["user_data"].items() if v}
            if row_count <= 3:
                print(f"📝 Sample event {row_count}: {json.dumps(event, indent=2)}")

            events.append(event)

    print(f"\n✅ Loaded {len(events)} valid events from {row_count} CSV rows")
    return events

# === PUSH TO META ===
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

# === MAIN ===
if __name__ == "__main__":
    if not META_ACCESS_TOKEN or not META_OFFLINE_EVENT_SET_ID or not DRIVE_FOLDER_ID:
        print("❌ Missing environment variable(s).")
        sys.exit(1)

    try:
        print("🔄 Starting RICS to Meta sync...")
        csv_path = download_latest_csv()
        events = load_rics_events(csv_path)

        if not events:
            print("❌ No valid events to sync. Aborting.")
            sys.exit(1)

        total = len(events)
        for i in range(0, total, BATCH_SIZE):
            batch = events[i:i + BATCH_SIZE]
            print(f"\n📦 Sending batch {i//BATCH_SIZE + 1}")
            push_to_meta(batch)

        print(f"\n🎉 Done. {len(events)} events attempted.")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
