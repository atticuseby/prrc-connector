from datetime import datetime
import os
import requests
import csv
from scripts.helpers import log_message
from scripts.config import OPTIMIZELY_API_TOKEN

RICS_API_TOKEN = OPTIMIZELY_API_TOKEN.strip()
RICS_API_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomer"


def fetch_rics_data():
    headers = {
        "Token": RICS_API_TOKEN,
        "Content-Type": "application/json"
    }

    print("🔍 Fetching all customers from RICS API...")

    output_dir = "./optimizely_connector/output"
    os.makedirs(output_dir, exist_ok=True)

    date_suffix = datetime.now().strftime('%m_%d_%Y')
    output_path = f"{output_dir}/rics_data_{date_suffix}.csv"

    seen_customers = set()
    all_customers = []
    page = 1
    max_pages = 100  # Safety guard against infinite loops

    while page <= max_pages:
        payload = {
            # Commenting out DOB filters to test total volume
            # "DateOfBirthStart": "1950-01-01",
            # "DateOfBirthEnd": "2025-12-31",
            "Page": page,
            "PageSize": 100
        }

        print(f"📦 Page {page}: Requesting data...")

        try:
            response = requests.post(RICS_API_URL, headers=headers, json=payload)
            response.raise_for_status()
        except requests.RequestException as e:
            log_message(f"❌ Error on page {page}: {e}")
            break

        data = response.json()
        print(f"📖 DEBUG raw response page {page}: {data}")

        if not data.get("IsSuccessful", False):
            log_message(f"❌ API failure on page {page}: {data.get('Message')}")
            break

        customers = data.get("Customers", [])
        print(f"📊 Page {page}: Retrieved {len(customers)} customers")

        if not customers:
            print("🛑 No more customers returned. Ending pagination.")
            break

        for c in customers:
            rics_id = c.get("CustomerId")
            if rics_id and rics_id not in seen_customers:
                seen_customers.add(rics_id)
                all_customers.append(c)

        page += 1

    print(f"📈 All customers pulled: {len(all_customers)} | Unique: {len(seen_customers)}")

    if not all_customers:
        raise Exception("❌ No customer data retrieved")

    print(f"💾 Writing final CSV to {output_path}")
    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "rics_id", "email", "first_name", "last_name",
            "orders", "total_spent", "city", "state", "zip"
        ])
        writer.writeheader()

        for c in all_customers:
            mailing = c.get("MailingAddress", {})
            writer.writerow({
                "rics_id": c.get("CustomerId"),
                "email": c.get("Email"),
                "first_name": c.get("FirstName"),
                "last_name": c.get("LastName"),
                "orders": c.get("OrderCount", 0),
                "total_spent": c.get("TotalSpent", 0.0),
                "city": mailing.get("City", ""),
                "state": mailing.get("State", ""),
                "zip": mailing.get("PostalCode", "")
            })

    log_message(f"✅ Saved {len(all_customers)} unique customers to {output_path}")


fetch_rics_data()
