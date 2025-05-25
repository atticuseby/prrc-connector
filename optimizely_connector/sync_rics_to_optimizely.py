# sync_rics_to_optimizely.py

import sys
import os
import csv
import requests
import math

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.config import OPTIMIZELY_API_TOKEN, DRY_RUN
from scripts.helpers import log_message

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/events"
BATCH_SIZE = 500

def run_sync():
    data_folder = "data"
    all_events = []

    # Read CSVs from /data
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_folder, filename)
            print(f"📂 Processing file: {filename}")
            with open(filepath, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    email = row.get("email", "").strip()
                    phone = row.get("phone", "").strip()
                    if not email and not phone:
                        log_message(f"❌ Skipping: no email or phone — {row}")
                        continue

                    identifiers = {}
                    if email:
                        identifiers["email"] = email
                    if phone:
                        identifiers["phone_number"] = phone

                    props = {
                        "first_name": row.get("first_name", "").strip(),
                        "last_name": row.get("last_name", "").strip(),
                        "city": row.get("city", "").strip(),
                        "state": row.get("state", "").strip(),
                        "zip": row.get("zip", "").strip(),
                        "rics_id": row.get("rics_id", "").strip(),
                        "orders": row.get("orders"),
                        "total_spent": row.get("total_spent")
                    }

                    # Add name if first and last present
                    if props["first_name"] and props["last_name"]:
                        props["name"] = f"{props['first_name']} {props['last_name']}"

                    # Remove empty/null values
                    props = {k: v for k, v in props.items() if v not in (None, "", "NULL")}

                    all_events.append({
                        "type": "customer_update",
                        "identifiers": identifiers,
                        "properties": props
                    })

    # Batch send
    total = len(all_events)
    total_batches = math.ceil(total / BATCH_SIZE)
    print(f"📦 Syncing {total} profiles in {total_batches} batch(es)")

    for i in range(total_batches):
        batch = all_events[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        if DRY_RUN:
            log_message(f"[DRY RUN] Batch {i+1}/{total_batches}: {batch}")
        else:
            try:
                response = requests.post(
                    OPTIMIZELY_ENDPOINT,
                    headers={
                        "x-api-key": OPTIMIZELY_API_TOKEN,
                        "Content-Type": "application/json"
                    },
                    json=batch,
                    timeout=10
                )
                if response.status_code in [200, 202]:
                    log_message(f"✅ Batch {i + 1}/{total_batches} accepted. Response: {response.status_code}")
                else:
                    log_message(f"❌ Batch {i + 1} failed. Status: {response.status_code} — {response.text}")
            except requests.exceptions.RequestException as e:
                log_message(f"❌ Network error in batch {i + 1}: {e}")

    print("\n✅ Optimizely sync process completed.")

if __name__ == "__main__":
    run_sync()
