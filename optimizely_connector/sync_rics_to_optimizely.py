# sync_rics_to_optimizely.py

import sys
import os
import csv
import requests
import math

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.config import OPTIMIZELY_API_TOKEN, DRY_RUN
from scripts.helpers import log_message

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/profiles"

BATCH_SIZE = 500

def run_sync():
    data_folder = "data"
    all_rows = []

    # Collect all rows from CSVs
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_folder, filename)
            print(f"📂 Processing file: {filename}")
            with open(filepath, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    email = row.get("email")
                    phone = row.get("phone")
                    if not email and not phone:
                        log_message(f"❌ No email or phone — skipping row: {row}")
                        continue

                    first_name = row.get("first_name", "").strip()
                    last_name = row.get("last_name", "").strip()

                    identifiers = []
                    if email:
                        identifiers.append({"type": "email", "value": email})
                    if phone:
                        identifiers.append({"type": "phone_number", "value": phone})

                    attributes = {
                        "first_name": first_name,
                        "last_name": last_name,
                        "orders": row.get("orders"),
                        "total_spent": row.get("total_spent"),
                        "city": row.get("city"),
                        "state": row.get("state"),
                        "zip": row.get("zip"),
                        "rics_id": row.get("rics_id")
                    }

                    if first_name and last_name:
                        attributes["name"] = f"{first_name} {last_name}"

                    attributes = {k: v for k, v in attributes.items() if v not in (None, "", "NULL")}

                    all_rows.append({
                        "identifiers": identifiers,
                        "attributes": attributes
                    })

    # Batch requests
    total_batches = math.ceil(len(all_rows) / BATCH_SIZE)
    print(f"📦 Syncing {len(all_rows)} profiles in {total_batches} batches of {BATCH_SIZE}")

    for i in range(total_batches):
        batch = all_rows[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        if DRY_RUN:
            log_message(f"[DRY RUN] Would send batch {i + 1}/{total_batches}: {batch}")
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
                    log_message(f"❌ Batch {i + 1} failed — Status: {response.status_code} — Response: {response.text}")
            except requests.exceptions.RequestException as e:
                log_message(f"❌ Network error in batch {i + 1}: {e}")

    print("\n✅ Optimizely sync process completed.")

if __name__ == "__main__":
    run_sync()
