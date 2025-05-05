# fetch_rics_data.py

import requests
import csv
import os
from scripts.helpers import log_message
from scripts.config import OPTIMIZELY_API_TOKEN as RICS_API_TOKEN

RICS_API_URL = "https://api.ricssoftware.com/v1/customers"  # Replace with real endpoint

def fetch_rics_data():
    headers = {
        "Authorization": f"Bearer {RICS_API_TOKEN}",
        "Content-Type": "application/json"
    }

    print("🔍 Sending request to RICS API...")
    response = requests.get(RICS_API_URL, headers=headers)

    if response.status_code != 200:
        log_message(f"❌ Failed to fetch RICS data — Status {response.status_code}")
        raise Exception("Failed RICS API pull")

    customers = response.json()
    print(f"📥 Pulled {len(customers)} customers from RICS")

    output_path = "data/rics_test_pull.csv"
    os.makedirs("data", exist_ok=True)

    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "rics_id", "email", "first_name", "last_name", 
            "orders", "total_spent", "city", "state", "zip"
        ])
        writer.writeheader()

        for c in customers:
            writer.writerow({
                "rics_id": c.get("id"),  # Confirm actual field names
                "email": c.get("email"),
                "first_name": c.get("first_name"),
                "last_name": c.get("last_name"),
                "orders": c.get("order_count"),
                "total_spent": c.get("total_spent"),
                "city": c.get("city"),
                "state": c.get("state"),
                "zip": c.get("zip")
            })

    log_message(f"✅ Saved test pull to {output_path}")
