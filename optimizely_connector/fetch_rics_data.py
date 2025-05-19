# optimizely_connector/fetch_rics_data.py

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

    payload = {
        "DateOfBirthStart": "1950-01-01",
        "DateOfBirthEnd": "2025-12-31",
        "Page": 1,
        "PageSize": 100
    }

    print("🔍 Fetching all customers from RICS API...")

    output_dir = "./optimizely_connector/output"
    os.makedirs(output_dir, exist_ok=True)

    date_suffix = datetime.now().strftime('%Y-%m-%d')
    output_path = f"{output_dir}/rics_data_{date_suffix}.csv"
    print(f"📝 Writing CSV to: {output_path}")

    seen_customers = set()
    page = 1
    total_fetched = 0

    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "rics_id", "email", "first_name", "last_name", 
            "orders", "total_spent", "city", "state", "zip"
        ])
        writer.writeheader()

        while True:
            payload["Page"] = page
            print(f"📦 Page {page}: Requesting customers...")

            try:
                response = requests.post(RICS_API_URL, headers=headers, json=payload)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                log_message(f"❌ Network error on page {page}: {e}")
                break

            data = response.json()

            if not data.get("IsSuccessful", False):
                print(f"⛔️ RICS API responded with failure on page {page} — exiting loop.")
                break

            customers = data.get("Customers", [])
            if not customers:
                print(f"🚫 No more customers found — finished at page {page}.")
                break

            print(f"📄 Page {page}: Retrieved {len(customers)} customers")
            total_fetched += len(customers)

            for c in customers:
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

            page += 1

    log_message(f"✅ Saved {len(seen_customers)} unique customers to {output_path}")


# 🔁 Trigger fetch if run as standalone script
fetch_rics_data()
