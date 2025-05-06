# fetch_rics_data.py

import requests
import csv
import os
from scripts.helpers import log_message
from scripts.config import OPTIMIZELY_API_TOKEN as RICS_API_TOKEN

RICS_API_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomer"

def fetch_rics_data():
    headers = {
        "Token": RICS_API_TOKEN,  # ✅ Correct header name
        "Content-Type": "application/json"
    }

    payload = {
        "CustomerID": None,
        "CustomerNumber": None,
        "Email": None,
        "FirstName": None,
        "LastName": None,
        "PhoneNumber": None
    }

    print("🔍 Sending POST request to RICS API...")
    try:
        response = requests.post(RICS_API_URL, headers=headers, json=payload)
    except requests.exceptions.RequestException as e:
        log_message(f"❌ Network error when connecting to RICS: {e}")
        raise

    if response.status_code != 200:
        log_message(f"❌ Failed to fetch RICS data — Status {response.status_code}")
        log_message(f"❌ RICS response: {response.text}")
        print(f"DEBUG RAW RICS RESPONSE: {response.text}")
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
                "rics_id": c.get("CustomerID"),
                "email": c.get("Email"),
                "first_name": c.get("FirstName"),
                "last_name": c.get("LastName"),
                "orders": c.get("OrderCount"),
                "total_spent": c.get("TotalSpent"),
                "city": c.get("City"),
                "state": c.get("State"),
                "zip": c.get("Zip")
            })

    log_message(f"✅ Saved test pull to {output_path}")
