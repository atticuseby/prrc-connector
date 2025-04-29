import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import csv
import requests
from scripts.config import OPTIMIZELY_API_TOKEN, DRY_RUN
from scripts.helpers import log_message

OPTIMIZELY_ENDPOINT = "https://api.optimizely.com/v1/recipients"  # Replace with correct endpoint if needed

def run_sync():
    data_folder = "data"
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_folder, filename)
            with open(filepath, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    if not row.get("email"):
                        log_message(f"❌ Missing email in row: {row}")
                        continue

                    payload = {
                        "customer_id": row.get("customer_id"),
                        "email": row.get("email"),
                        "orders": row.get("orders"),
                        "total_spent": row.get("total_spent"),
                    }

                    if DRY_RUN:
                        log_message(f"[DRY RUN] Would send: {payload}")
                    else:
                        response = requests.post(
                            OPTIMIZELY_ENDPOINT,
                            headers={"Authorization": f"Bearer {OPTIMIZELY_API_TOKEN}"},
                            json=payload
                        )
                        log_message(f"Sent {row.get('email')} — Status: {response.status_code}")

    print("\n✅ Data sync to Optimizely completed.")
