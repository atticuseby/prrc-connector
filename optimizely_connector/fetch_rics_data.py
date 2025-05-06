# fetch_rics_data.py

import requests
import csv
import os
from scripts.helpers import log_message
from scripts.config import OPTIMIZELY_API_TOKEN as RICS_API_TOKEN

# ✅ Correct RICS API endpoint
RICS_API_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomer"

def fetch_rics_data():
    headers = {
        "Authorization": f"Bearer {RICS_API_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {}  # Start with an empty payload — we'll adjust based on API response

    print("🔍 Sending POST request to RICS API...")
    try:
        response = requests.post(RICS_API_URL, headers=headers, json=payload)
    except requests.exceptions.RequestException as e:
        log_message(f"❌ Network error when connecting to RICS: {e}")
        raise

    if response.status_code != 200:
        log_message(f"❌ Failed to fetch RICS data — Status {response.status_code}")
        log_message(f"❌ RICS response: {response.text}")
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
                "rics_id": c.get("id"),
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
