import os
import csv
import time
import requests
from datetime import datetime
from scripts.config import RICS_API_TOKEN, TEST_EMAIL
from scripts.helpers import log_message

MAX_SKIP = 100  # Can increase for full production
MAX_RETRIES = 3
MAX_RESPONSE_TIME_SECONDS = 60  # Only warn if exceeded — don’t abort
ABSOLUTE_TIMEOUT_SECONDS = 120  # This ensures the request doesn’t hang forever

data_fields = [
    "rics_id", "email", "first_name", "last_name", "orders",
    "total_spent", "city", "state", "zip", "phone"
]

def fetch_rics_data():
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_export_{timestamp}.csv"
    output_path = os.path.join("optimizely_connector", "output", filename)
    data_path = os.path.join("data", filename)

    os.makedirs("optimizely_connector/output", exist_ok=True)
    os.makedirs("data", exist_ok=True)

    all_rows = []
    skip = 0

    while skip < MAX_SKIP:
        log_message(f"\n📄 Requesting customers from skip: {skip}...")

        attempt = 0
        customers = []

        while attempt < MAX_RETRIES:
            try:
                start = time.time()
                response = requests.post(
                    url="https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                    headers={"Authorization": f"Bearer {RICS_API_TOKEN}"},
                    json={"StoreCode": 12132, "Skip": skip, "Take": 100},
                    timeout=ABSOLUTE_TIMEOUT_SECONDS
                )
                duration = time.time() - start
                log_message(f"⏱️ Attempt {attempt+1} response time: {duration:.2f}s")

                response.raise_for_status()
                customers = response.json().get("Customers", [])

                if duration > MAX_RESPONSE_TIME_SECONDS:
                    log_message(f"⚠️ Warning: Response exceeded {MAX_RESPONSE_TIME_SECONDS}s, but continuing anyway.")

                if customers:
                    break  # Success
                else:
                    log_message(f"⚠️ No customers returned on attempt {attempt+1}. Retrying...")
                    attempt += 1
                    time.sleep(3)

            except Exception as e:
                log_message(f"❌ Attempt {attempt+1} failed: {e}")
                attempt += 1
                time.sleep(3)

        if attempt == MAX_RETRIES:
            log_message("❌ Aborting: RICS failed or unresponsive after 3 attempts.")
            raise SystemExit(1)

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

    log_message(f"\n🔧 Appending test profiles (x3)...")
    all_rows.extend([
        {"rics_id": "test-001", "email": TEST_EMAIL, "first_name": "Test", "last_name": "Email", "orders": 1, "total_spent": 10, "city": "Testville", "state": "TN", "zip": "37201", "phone": ""},
        {"rics_id": "test-002", "email": "", "first_name": "Phone", "last_name": "Only", "orders": 0, "total_spent": 0, "city": "Franklin", "state": "TN", "zip": "37064", "phone": "5551234567"},
        {"rics_id": "test-003", "email": "test+both@bandit.com", "first_name": "Dual", "last_name": "Contact", "orders": 2, "total_spent": 200, "city": "Memphis", "state": "TN", "zip": "38103", "phone": "5559876543"}
    ])

    for path in [output_path, data_path]:
        with open(path, mode="w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=data_fields)
            writer.writeheader()
            writer.writerows(all_rows)
        log_message(f"📝 Wrote CSV to: {path}")

    return output_path

if __name__ == "__main__":
    fetch_rics_data()
