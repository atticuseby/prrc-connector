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

    all_customers = set()
    page = 1
    page_size = 100
    max_retries = 5
    retry_delay = 5
    timestamp = datetime.now().strftime("%Y-%m-%d")
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/rics_customers_{timestamp}.csv"

    print("🔍 Starting paginated RICS customer sync...")

    with open(output_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["CustomerId", "AccountNumber", "FirstName", "LastName", "Email", "PhoneNumber", "DateOfBirth", "Address", "City", "State", "PostalCode"])
        
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
                raise Exception("RICS API returned unsuccessful status")

            customers = data.get("Customers", [])
            for c in customers:
                customer_id = c.get("CustomerId")
                if customer_id not in all_customers:
                    all_customers.add(customer_id)
                    mailing = c.get("MailingAddress", {})
                    writer.writerow([
                        c.get("CustomerId"),
                        c.get("AccountNumber"),
                        c.get("FirstName"),
                        c.get("LastName"),
                        c.get("Email"),
                        c.get("PhoneNumber"),
                        c.get("DateOfBirth"),
                        mailing.get("Address", ""),
                        mailing.get("City", ""),
                        mailing.get("State", ""),
                        mailing.get("PostalCode", "")
                    ])

            if len(customers) < page_size:
                print("✅ Reached the final page of customers.")
                break

            page += 1

    log_message(f"✅ Saved RICS customer export to {output_path}")
    print(f"✅ Pulled {len(all_customers)} unique customers total.")
