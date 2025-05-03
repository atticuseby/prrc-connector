# sync_rics_to_optimizely.py

import sys
import os
import csv
import requests

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.config import OPTIMIZELY_API_TOKEN, DRY_RUN
from scripts.helpers import log_message

OPTIMIZELY_ENDPOINT = "https://api.optimizely.com/v3/profiles"


def run_sync():
    data_folder = "data"
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_folder, filename)
            with open(filepath, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    rics_id = row.get("rics_id")
                    if not rics_id:
                        log_message(f"❌ Missing RICS ID in row: {row}")
                        continue

                    payload = {
                        "identifiers": [
                            {"type": "rics_id", "value": rics_id}
                        ],
                        "attributes": {
                            "email": row.get("email"),
                            "first_name": row.get("first_name"),
                            "last_name": row.get("last_name"),
                            "orders": row.get("orders"),
                            "total_spent": row.get("total_spent"),
                            "city": row.get("city"),
                            "state": row.get("state"),
                            "zip": row.get("zip")
                        }
                    }

                    if DRY_RUN:
                        log_message(f"[DRY RUN] Would send: {payload}")
                    else:
                        response = requests.post(
                            OPTIMIZELY_ENDPOINT,
                            headers={"Authorization": f"Bearer {OPTIMIZELY_API_TOKEN}"},
                            json=payload
                        )
                        log_message(f"Sent profile with RICS ID {rics_id} — Status: {response.status_code}")

    print("\n✅ Data sync to Optimizely completed.")
