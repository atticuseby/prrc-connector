# fetch_rics_data.py

import os
import csv
import requests
from datetime import datetime
from scripts.config import RICS_API_TOKEN, TEST_EMAIL
from scripts.helpers import log_message

MAX_SKIP = 100  # Keep small during test mode
data_fields = [
    "rics_id", "email", "first_name", "last_name", "orders",
    "total_spent", "city", "state", "zip", "phone"
]

def fetch_rics_data():
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"{timestamp}_rics_data.csv"
    output_path = os.path.join("./optimizely_connector/output", filename)
    data_path = os.path.join("data", filename)

    os.makedirs("./optimizely_connector/output", exist_ok=True)
    os.makedirs("data", exist_ok=True)

    all_rows = []
    skip = 0

    while skip < MAX_SKIP:
        log_message(f"\n📄 Requesting customers from skip: {skip}...")
        try:
            response = requests.post(
                url="https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                headers={"Authorization": f"Bearer {RICS_API_TOKEN}"},
                json={"StoreCode": 12132, "Skip": skip, "Take": 100},
                timeout=20
            )
            response.raise_for_status()
            customers = response.json().get("Customers", [])
        except Exception as e:
            log_message(f"❌ Failed to retrieve data: {e}")
            break

        if not customers:
            break

        for customer in customers:
            mailing = customer.get("MailingAddress", {})
            row = {
                "rics_id": customer.get("CustomerId"),
                "email": customer.get("Email", "").strip(),
                "first_name": customer.get("FirstName", "").strip(),
                "last_name": customer.get("LastName", "").strip(),
                "orders": customer.get("OrderCount", 0),
                "total_spent": customer.get("TotalSpent", 0),
                "city": mailing.get("City", "").strip(),
                "state": mailing.get("State", "").strip(),
                "zip": mailing.get("PostalCode", "").strip(),
                "phone": customer.get("PhoneNumber", "").strip()
            }
            all_rows.append(row)
        skip += 100

    # Inject 3 fake test profiles
    log_message(f"\n🔧 Appending test profiles (x3)...")
    all_rows.extend([
        {
            "rics_id": "test-001",
            "email": TEST_EMAIL,
            "first_name": "Test",
            "last_name": "Email",
            "orders": 1,
            "total_spent": 10,
            "city": "Testville",
            "state": "TN",
            "zip": "37201",
            "phone": ""
        },
        {
            "rics_id": "test-002",
            "email": "",
            "first_name": "Phone",
            "last_name": "Only",
            "orders": 0,
            "total_spent": 0,
            "city": "Franklin",
            "state": "TN",
            "zip": "37064",
            "phone": "5551234567"
        },
        {
            "rics_id": "test-003",
            "email": "test+both@bandit.com",
            "first_name": "Dual",
            "last_name": "Contact",
            "orders": 2,
            "total_spent": 200,
            "city": "Memphis",
            "state": "TN",
            "zip": "38103",
            "phone": "5559876543"
        }
    ])

    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data_fields)
        writer.writeheader()
        writer.writerows(all_rows)
    log_message(f"📝 Writing final CSV to: {output_path}")

    with open(data_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=data_fields)
        writer.writeheader()
        writer.writerows(all_rows)
    log_message(f"📂 Copied CSV to /data/: {data_path}")

if __name__ == "__main__":
    fetch_rics_data()
