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
    seen_ids = set()
    max_pages = 100
    print("🔍 Fetching all customers from RICS API...")

    for page in range(1, max_pages + 1):
        payload = {
            "DateOfBirthStart": "1950-01-01",
            "DateOfBirthEnd": "2025-12-31",
            "Page": page,
            "PageSize": 100
        }

        try:
            response = requests.post(RICS_API_URL, headers=headers, json=payload)
        except requests.RequestException as e:
            log_message(f"❌ Network error on page {page}: {e}")
            break

        if response.status_code != 200:
            log_message(f"❌ Page {page} returned status {response.status_code}")
            break

        data = response.json()
        if not data.get("IsSuccessful", False):
            log_message(f"❌ Page {page} returned unsuccessful status: {data.get('Message')}")
            break

        customers = data.get("Customers", [])
        if not customers:
            print(f"✅ No more customers after page {page - 1}")
            break

        print(f"📦 Page {page}: Retrieved {len(customers)} customers")

        for c in customers:
            rics_id = c.get("CustomerId")
            if rics_id and rics_id not in seen_ids:
                seen_ids.add(rics_id)
                all_customers.append({
                    "rics_id": rics_id,
                    "email": c.get("Email"),
                    "first_name": c.get("FirstName"),
                    "last_name": c.get("LastName"),
                    "orders": c.get("OrderCount", 0),
                    "total_spent": c.get("TotalSpent", 0.0),
                    "city": c.get("MailingAddress", {}).get("City", ""),
                    "state": c.get("MailingAddress", {}).get("State", ""),
                    "zip": c.get("MailingAddress", {}).get("PostalCode", "")
                })

    if not all_customers:
        log_message("⚠️ No customers retrieved — skipping CSV export")
        return

    output_dir = "./optimizely_connector/output"
    os.makedirs(output_dir, exist_ok=True)
    date_suffix = datetime.now().strftime('%Y-%m-%d')
    output_path = f"{output_dir}/rics_data_{date_suffix}.csv"

    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "rics_id", "email", "first_name", "last_name",
            "orders", "total_spent", "city", "state", "zip"
        ])
        writer.writeheader()
        for c in all_customers:
            writer.writerow(c)

    log_message(f"✅ Saved {len(all_customers)} customers to {output_path}")

fetch_rics_data()
