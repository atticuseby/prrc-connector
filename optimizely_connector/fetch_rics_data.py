# fetch_rics_data.py

import requests
import csv
import os
from datetime import datetime
from scripts.helpers import log_message
from scripts.config import RICS_API_TOKEN, TEST_EMAIL

RICS_API_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomer"

mock_path = "./optimizely_connector/output/mock_rics_export.csv"
if os.path.exists(mock_path):
    os.remove(mock_path)
    print("🧹 Removed old mock_rics_export.csv")

def fetch_rics_data():
    headers = {
        "Token": RICS_API_TOKEN.strip(),
        "Content-Type": "application/json"
    }

    all_customers = []
    skip = 0
    take = 100

    print(f"\n🕒 {datetime.now().isoformat()} — Starting customer fetch from RICS")

    while True:
        payload = {
            "StoreCode": 12132,
            "Skip": skip,
            "Take": take,
            "FirstName": "%"
        }

        print(f"📄 Requesting customers from skip: {skip}...")

        try:
            response = requests.post(RICS_API_URL, headers=headers, json=payload)
        except requests.exceptions.RequestException as e:
            log_message(f"❌ Network error: {e}")
            break

        print(f"📖 DEBUG raw response: {response.text[:300]}... [truncated]")

        if response.status_code != 200:
            log_message(f"❌ Bad status: {response.status_code}")
            break

        data = response.json()
        if not data.get("IsSuccessful", False):
            log_message(f"❌ API failure: {data.get('Message')} | {data.get('ValidationMessages')}")
            break

        customers = data.get("Customers", [])
        if not customers:
            print("🚫 No customers returned — pagination complete.")
            break

        print(f"📦 Retrieved {len(customers)} customers")

        for c in customers:
            all_customers.append(c)

        skip += take

    print(f"\n📊 Total unique customers exported: {len(all_customers)}")

    if not all_customers:
        raise Exception("❌ No usable customer data found")

    output_dir = "./optimizely_connector/output"
    os.makedirs(output_dir, exist_ok=True)

    filename = datetime.now().strftime("%m_%d_%Y_%H%M_rics_data.csv")
    output_path = os.path.join(output_dir, filename)

    print(f"📝 Writing final CSV to: {output_path}")

    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "rics_id", "email", "phone", "first_name", "last_name",
            "orders", "total_spent", "city", "state", "zip"
        ])
        writer.writeheader()

        for c in all_customers:
            mailing = c.get("MailingAddress", {})
            writer.writerow({
                "rics_id": c.get("CustomerId"),
                "email": c.get("Email"),
                "phone": c.get("PhoneNumber"),
                "first_name": c.get("FirstName"),
                "last_name": c.get("LastName"),
                "orders": c.get("OrderCount", 0),
                "total_spent": c.get("TotalSpent", 0.0),
                "city": mailing.get("City", ""),
                "state": mailing.get("State", ""),
                "zip": mailing.get("PostalCode", "")
            })

        if TEST_EMAIL:
            print(f"🔧 Appending test profile with email: {TEST_EMAIL}")
            writer.writerow({
                "rics_id": "test-rics-id",
                "email": TEST_EMAIL,
                "phone": "",
                "first_name": "Test",
                "last_name": "User",
                "orders": 0,
                "total_spent": 0,
                "city": "Nashville",
                "state": "TN",
                "zip": "37201"
            })

    data_dir = "./data"
    os.makedirs(data_dir, exist_ok=True)
    dest_path = os.path.join(data_dir, os.path.basename(output_path))
    os.system(f"cp {output_path} {dest_path}")
    print(f"📂 Copied CSV to /data/: {os.path.basename(output_path)}")
