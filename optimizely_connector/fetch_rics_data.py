# fetch_rics_data.py

import requests
import csv
import os
import sys
from base64 import b64encode

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.helpers import log_message
from scripts.config import OPTIMIZELY_API_TOKEN as PLACEHOLDER_API_TOKEN

RICS_API_URL = "https://api.ricssoftware.com/v1/customers"

def try_rics_auth():
    test_results = []

    # Common guesses
    headers_list = [
        {
            "name": "Bearer Token",
            "headers": {
                "Authorization": f"Bearer {PLACEHOLDER_API_TOKEN}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            "url": RICS_API_URL
        },
        {
            "name": "API Key Header",
            "headers": {
                "X-RICS-API-KEY": PLACEHOLDER_API_TOKEN,
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            "url": RICS_API_URL
        },
        {
            "name": "Account in Query Param",
            "headers": {
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            "url": f"{RICS_API_URL}?account=12132"
        },
        {
            "name": "Basic Auth Header",
            "headers": {
                "Authorization": f"Basic {b64encode(f'{PLACEHOLDER_API_TOKEN}:dummy'.encode()).decode()}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            "url": RICS_API_URL
        }
    ]

    for attempt in headers_list:
        print(f"\n🔐 Attempting: {attempt['name']}")
        try:
            response = requests.get(attempt["url"], headers=attempt["headers"])
            print(f"🔁 Status: {response.status_code}")
            print(f"🧾 Response: {response.text[:250]}")  # Preview top of body

            test_results.append({
                "method": attempt["name"],
                "status": response.status_code,
                "body_snippet": response.text[:250]
            })

            if response.status_code == 200:
                return response.json()

        except Exception as e:
            print(f"❌ {attempt['name']} failed: {e}")

    log_message("❌ All auth attempts failed.")
    return None

def fetch_rics_data():
    customers = try_rics_auth()
    if not customers:
        print("⚠️ Could not authenticate with RICS API.")
        return

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
