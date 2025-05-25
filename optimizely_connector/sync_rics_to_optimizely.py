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

                    identifiers = {}
                    if email:
                        identifiers["email"] = email
                    if phone:
                        identifiers["phone_number"] = phone

                    attributes = {
                        "first_name": row.get("first_name", "").strip(),
                        "last_name": row.get("last_name", "").strip(),
                        "orders": row.get("orders"),
                        "total_spent": row.get("total_spent"),
                        "city": row.get("city"),
                        "state": row.get("state"),
                        "zip": row.get("zip"),
                        "rics_id": row.get("rics_id")
                    }

                    first = attributes.get("first_name")
                    last = attributes.get("last_name")
                    if first and last:
                        attributes["name"] = f"{first} {last}"

                    attributes = {k: v for k, v in attributes.items() if v not in (None, "", "NULL")}

                    all_rows.append({
                        "type": "customer_update",
                        "identifiers": identifiers,
                        "properties": attributes
                    })

    # Add test cases for verification
    test_profiles = [
        {"rics_id": "test-001", "email": "test+emailonly@bandit.com", "phone": "", "first_name": "Email", "last_name": "Only", "orders": 1, "total_spent": 10, "city": "Nashville", "state": "TN", "zip": "37201"},
        {"rics_id": "test-002", "email": "", "phone": "5551234567", "first_name": "Phone", "last_name": "Only", "orders": 0, "total_spent": 0, "city": "Franklin", "state": "TN", "zip": "37064"},
        {"rics_id": "test-003", "email": "test+both@bandit.com", "phone": "5559876543", "first_name": "Dual", "last_name": "Contact", "orders": 2, "total_spent": 200, "city": "Memphis", "state": "TN", "zip": "38103"},
        {"rics_id": "test-004", "email": "test+missing@bandit.com", "phone": "", "first_name": "No", "last_name": "City", "orders": 0, "total_spent": 0, "city": "", "state": "", "zip": ""},
        {"rics_id": "test-005", "email": "", "phone": "", "first_name": "Null", "last_name": "Data", "orders": 0, "total_spent": 0, "city": "Nowhere", "state": "ZZ", "zip": "00000"},
        {"rics_id": "test-006", "email": "bademail@", "phone": "", "first_name": "Bad", "last_name": "Email", "orders": 0, "total_spent": 0, "city": "Errorville", "state": "ER", "zip": "12345"},
        {"rics_id": "test-007", "email": "test+blanknames@bandit.com", "phone": "", "first_name": "", "last_name": "", "orders": 1, "total_spent": 50, "city": "Knoxville", "state": "TN", "zip": "37902"}
    ]

    for profile in test_profiles:
        email = profile.get("email")
        phone = profile.get("phone")
        if not email and not phone:
            continue

        identifiers = {}
        if email:
            identifiers["email"] = email
        if phone:
            identifiers["phone_number"] = phone

        attributes = {k: v for k, v in profile.items() if k not in ("email", "phone") and v not in (None, "", "NULL")}
        first = profile.get("first_name", "").strip()
        last = profile.get("last_name", "").strip()
        if first and last:
            attributes["name"] = f"{first} {last}"

        all_rows.append({
            "type": "customer_update",
            "identifiers": identifiers,
            "properties": attributes
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
