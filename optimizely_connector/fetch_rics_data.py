import requests
import csv
import os
import time
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
    page_size = 1000
    max_retries = 5
    retry_delay = 5

    print("🔍 Starting paginated RICS customer sync...")

    while True:
        payload = {
            "DateOfBirthStart": "1950-01-01",
            "DateOfBirthEnd": "2025-12-31",
            "Page": page,
            "PageSize": page_size
        }

        for attempt in range(1, max_retries + 1):
            try:
                response = requests.post(RICS_API_URL, headers=headers, json=payload)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    wait_time = retry_delay * (2 ** (attempt - 1))
                    log_message(f"⚠️ Rate limited (attempt {attempt}/{max_retries}). Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    log_message(f"❌ RICS API request failed on page {page}: {e}")
                    raise
        else:
            log_message(f"❌ Max retries reached on page {page}. Exiting.")
            raise Exception("Max retries reached. Aborting.")

        data = response.json()

        if not data.get("IsSuccessful", False):
            log_message(f"❌ RICS API returned failure on page {page}")
            break

        customers = data.get("Customers", [])

        if not customers:
            log_message(f"⚠️ No customers found on page {page}. Stopping sync.")
            break

        all_customers.extend(customers)
        log_message(f"📄 Pulled page {page} — {len(customers)} customers")

        page += 1

    print(f"\n📥 Pulled {len(all_customers)} total customers from RICS")

    # Save results to CSV
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/rics_customers_{timestamp}.csv"

    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "CustomerId", "AccountNumber", "FirstName", "LastName",
            "Email", "PhoneNumber", "DateOfBirth", "Address",
            "City", "State", "PostalCode"
        ])
        writer.writeheader()

        for c in all_customers:
            mailing = c.get("MailingAddress", {})
            writer.writerow({
                "CustomerId": c.get("CustomerId"),
                "AccountNumber": c.get("AccountNumber"),
                "FirstName": c.get("FirstName"),
                "LastName": c.get("LastName"),
                "Email": c.get("Email"),
                "PhoneNumber": c.get("PhoneNumber"),
                "DateOfBirth": c.get("DateOfBirth"),
                "Address": mailing.get("Address", ""),
                "City": mailing.get("City", ""),
                "State": mailing.get("State", ""),
                "PostalCode": mailing.get("PostalCode", "")
            })

    log_message(f"✅ Saved RICS customer export to {output_path}")
