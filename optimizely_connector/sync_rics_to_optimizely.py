import sys
import os
import csv
import requests

# Make sure scripts/ is in path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.config import OPTIMIZELY_API_TOKEN, DRY_RUN
from scripts.helpers import log_message

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/profiles"

def run_sync():
    data_folder = "data"

    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_folder, filename)
            print(f"\U0001f4c2 Processing file: {filename}")

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

                    if DRY_RUN:
                        log_message(f"[DRY RUN] Would send to Optimizely:\n{payload}")
                    else:
                        try:
                            response = requests.post(
                                OPTIMIZELY_ENDPOINT,
                                headers={
                                    "x-api-key": OPTIMIZELY_API_TOKEN,
                                    "Content-Type": "application/json"
                                },
                                json=payload,
                                timeout=10
                            )
                            log_message(f"🔍 Syncing: {email}")

                            if response.status_code == 200:
                                log_message(f"✅ Success:\n↪️ Payload: {payload}\n↪️ Response: {response.text}")
                            else:
                                log_message(
                                    f"❌ Failed:\n↪️ Status: {response.status_code}\n↪️ Response: {response.text}\n↪️ Payload: {payload}"
                                )
                        except requests.exceptions.RequestException as e:
                            log_message(f"❌ Network error for {email}: {e}")

    print("\n✅ Optimizely sync process completed.")

if __name__ == "__main__":
    run_sync()
