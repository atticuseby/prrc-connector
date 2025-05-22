import requests
import csv
import os
from datetime import datetime
from scripts.helpers import log_message
from scripts.config import OPTIMIZELY_API_TOKEN

RICS_API_TOKEN = OPTIMIZELY_API_TOKEN.strip()
RICS_API_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomer"

def fetch_rics_data():
    headers = {
        "Token": RICS_API_TOKEN,
        "Content-Type": "application/json"
    }

    all_customers = []
    page = 1
    page_size = 100

    print("🔍 Fetching all customers from RICS API...")

    while True:
        payload = {
            "DateOfBirthStart": "1950-01-01",
            "DateOfBirthEnd": "2025-12-31",
            "Page": page,
            "PageSize": page_size
        }

        try:
            response = requests.post(RICS_API_URL, headers=headers, json=payload)
        except requests.exceptions.RequestException as e:
            log_message(f"❌ Network error when connecting to RICS: {e}")
            raise

        if response.status_code != 200:
            log_message(f"❌ Failed to fetch RICS data — Status {response.status_code}")
            raise Exception("Failed RICS API pull")

        data = response.json()

        if not data.get("IsSuccessful", False):
            log_message("❌ RICS API responded with failure")
            raise Exception("RICS API returned unsuccessful status")

        customers = data.get("Customers", [])
        print(f"📦 Page {page}: Retrieved {len(customers)} customers")
        all_customers.extend(customers)

        if len(customers) < page_size:
            break  # Done paginating

        page += 1

    print(f"✅ Total customers fetched: {len(all_customers)}")

    output_dir = "./optimizely_connector/output"
    os.makedirs(output_dir, exist_ok=True)

    date_suffix = datetime.now().strftime('%m_%d_%Y')
    output_path = f"{output_dir}/{date_suffix}_rics_data.csv"

    print(f"📝 Writing CSV to: {output_path}")

    seen_customers = set()
    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "rics_id", "email", "first_name", "last_name", 
            "orders", "total_spent", "city", "state", "zip"
        ])
        writer.writeheader()

        for c in all_customers:
            rics_id = c.get("CustomerId")
            if rics_id not in seen_customers:
                seen_customers.add(rics_id)
                mailing = c.get("MailingAddress", {})
                writer.writerow({
                    "rics_id": rics_id,
                    "email": c.get("Email"),
                    "first_name": c.get("FirstName"),
                    "last_name": c.get("LastName"),
                    "orders": c.get("OrderCount", 0),
                    "total_spent": c.get("TotalSpent", 0.0),
                    "city": mailing.get("City", ""),
                    "state": mailing.get("State", ""),
                    "zip": mailing.get("PostalCode", "")
                })

    log_message(f"✅ Saved complete RICS export to {output_path}")

fetch_rics_data()
