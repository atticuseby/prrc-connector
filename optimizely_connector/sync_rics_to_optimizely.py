import sys
import os
import csv
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.config import OPTIMIZELY_API_TOKEN, DRY_RUN
from scripts.helpers import log_message

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/profiles"

def run_sync():
    data_folder = "data"
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_folder, filename)
            print(f"📂 Processing file: {filename}")
            with open(filepath, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    email = row.get("email")
                    if not email:
                        log_message(f"❌ Missing email — skipping row: {row}")
                        continue

                    attributes = {
                        "first_name": row.get("first_name"),
                        "last_name": row.get("last_name"),
                        "orders": row.get("orders"),
                        "total_spent": row.get("total_spent"),
                        "city": row.get("city"),
                        "state": row.get("state"),
                        "zip": row.get("zip"),
                        "rics_id": row.get("rics_id")
                    }
                    attributes = {k: v for k, v in attributes.items() if v not in (None, "", "NULL")}

                    payload = {
                        "identifiers": [
                            {"type": "email", "value": email}
                        ],
                        "attributes": attributes
                    }

                    print(f"🔍 Syncing: {email}")
                    if DRY_RUN:
                        log_message(f"[DRY RUN] Would send to Optimizely: {payload}")
                    else:
                        try:
                            response = requests.post(
                                OPTIMIZELY_ENDPOINT,
                                headers={
                                    "Authorization": f"Bearer {OPTIMIZELY_API_TOKEN}",
                                    "Content-Type": "application/json"
                                },
                                json=payload,
                                timeout=10
                            )
                            if response.status_code == 200:
                                log_message(f"✅ Synced profile for: {email}")
                                print(f"↪️ Response: {response.text}")
                            else:
                                log_message(f"❌ Failed to sync {email} — Status: {response.status_code}")
                                print(f"↪️ Error response: {response.text}")
                        except requests.exceptions.RequestException as e:
                            log_message(f"❌ Network error for {email}: {e}")
                            print(f"⚠️ Exception for {email}: {e}")

    print("\n✅ Optimizely sync process completed.")

if __name__ == "__main__":
    run_sync()
