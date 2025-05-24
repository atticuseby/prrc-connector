import requests
import csv
import os
import glob
import shutil
from datetime import datetime
from scripts.helpers import log_message
from scripts.config import OPTIMIZELY_API_TOKEN

RICS_API_TOKEN = OPTIMIZELY_API_TOKEN.strip()
RICS_API_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomer"
TEST_EMAIL = os.getenv("TEST_EMAIL", "youremail@yourdomain.com").strip()

# 🔥 Remove any old mock file if present
mock_path = "./optimizely_connector/output/mock_rics_export.csv"
if os.path.exists(mock_path):
    os.remove(mock_path)
    print("🧹 Removed old mock_rics_export.csv")

def fetch_rics_data():
    headers = {
        "Token": RICS_API_TOKEN,
        "Content-Type": "application/json"
    }

    all_customers = []
    seen_customers = set()
    skip = 0
    take = 100
    max_failures = 3
    failures = 0

    max_skip = 300  # Test mode: limit to 300 customers

    print(f"\n🕒 {datetime.now().isoformat()} — Starting customer fetch from RICS")

    while True:
        if max_skip is not None and skip >= max_skip:
            print("⏹️ Reached cap defined by max_skip — ending test mode run.")
            break

        payload = {
            "StoreCode": 12132,
            "Skip": skip,
            "Take": take,
            "FirstName": "%"  # Wildcard
        }

        print(f"📄 Requesting customers from skip: {skip}...")

        try:
            response = requests.post(RICS_API_URL, headers=headers, json=payload, timeout=10)
        except requests.exceptions.RequestException as e:
            log_message(f"❌ Network error: {e}")
            failures += 1
            if failures >= max_failures:
                raise Exception("❌ Max retries hit. Aborting.")
            continue

        print(f"📖 DEBUG raw response: {response.text[:300]}... [truncated]")

        if response.status_code != 200:
            log_message(f"❌ Bad status: {response.status_code}")
            failures += 1
            if failures >= max_failures:
                raise Exception("❌ Max retries hit. Aborting.")
            continue

        data = response.json()

        if not data.get("IsSuccessful", False):
            log_message(f"❌ API failure: {data.get('Message')} | {data.get('ValidationMessages')}")
            failures += 1
            if failures >= max_failures:
                raise Exception("❌ Max retries hit due to validation.")
            continue

        customers = data.get("Customers", [])
        if not customers:
            print("🚫 No customers returned — pagination complete.")
            break

        print(f"📦 Retrieved {len(customers)} customers")

        for c in customers:
            rics_id = c.get("CustomerId")
            email = c.get("Email")
            if rics_id and rics_id not in seen_customers and email:
                seen_customers.add(rics_id)
                all_customers.append(c)

        skip += take
        failures = 0

    print(f"\n📊 Total unique customers exported: {len(seen_customers)}")

    if not all_customers:
        raise Exception("❌ No usable customer data found")

    output_dir = "./optimizely_connector/output"
    os.makedirs(output_dir, exist_ok=True)

    filename = datetime.now().strftime("%m_%d_%Y_%H%M_rics_data.csv")
    output_path = os.path.join(output_dir, filename)

    print(f"📝 Writing final CSV to: {output_path}")

        with open(output_path, mode="w", newline="") as file:
        fieldnames = [
            "rics_id", "email", "first_name", "last_name",
            "orders", "total_spent", "city", "state", "zip"
        ]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
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

        # ✅ Append the test record BEFORE closing
        print(f"🔧 Appending test profile with email: {TEST_EMAIL}")
        writer.writerow({
            "rics_id": "test-rics-id",
            "email": TEST_EMAIL,
            "first_name": "Test",
            "last_name": "User",
            "orders": 0,
            "total_spent": 0.0,
            "city": "Nashville",
            "state": "TN",
            "zip": "37201"
        })

    log_message(f"✅ Export complete: {output_path}")

    # ✅ Copy the CSV to /data for Optimizely sync
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    shutil.copy(output_path, os.path.join(data_dir, os.path.basename(output_path)))
    print(f"📂 Copied CSV to /data/: {os.path.basename(output_path)}")
