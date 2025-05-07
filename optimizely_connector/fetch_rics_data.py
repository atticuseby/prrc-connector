import requests
import csv
import os
from scripts.helpers import log_message
from scripts.config import OPTIMIZELY_API_TOKEN

RICS_API_TOKEN = OPTIMIZELY_API_TOKEN.strip()
RICS_API_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomer"

def fetch_rics_data():
    headers = {
        "Token": RICS_API_TOKEN,
        "Content-Type": "application/json"
    }

    # ✅ Pull a single page of customers for testing
    payload = {
        "DateOfBirthStart": "1950-01-01",
        "DateOfBirthEnd": "2025-12-31",
        "Page": 1,
        "PageSize": 100
    }

    print("🔍 Sending POST request to RICS API...")
    try:
        response = requests.post(RICS_API_URL, headers=headers, json=payload)
    except requests.exceptions.RequestException as e:
        log_message(f"❌ Network error when connecting to RICS: {e}")
        raise

    # Log raw response for debugging
    print(f"DEBUG RAW RICS RESPONSE: {response.text}")

    if response.status_code != 200:
        log_message(f"❌ Failed to fetch RICS data — Status {response.status_code}")
        raise Exception("Failed RICS API pull")

    data = response.json()

    if not data.get("IsSuccessful", False):
        log_message("❌ RICS API responded with failure")
        raise Exception("RICS API returned unsuccessful status")

    customers = data.get("Customers", [])
    print(f"📥 Pulled {len(customers)} customers from RICS")

    # Save the data to a CSV file
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
                "rics_id": c.get("CustomerId"),
                "email": c.get("Email"),
                "first_name": c.get("FirstName"),
                "last_name": c.get("LastName"),
                "orders": c.get("OrderCount", 0),
                "total_spent": c.get("TotalSpent", 0.0),
                "city": c.get("City", ""),
                "state": c.get("State", ""),
                "zip": c.get("Zip", "")
            })

    log_message(f"✅ Saved test pull to {output_path}")


fetch_rics_data()
