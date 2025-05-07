import requests
import csv
import os
import time
import random
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
                data = response.json()
                
                if not data.get("IsSuccessful", False):
                    print(f"❌ RICS API returned failure on page {page}")
                    raise Exception("RICS API returned unsuccessful status")
                
                customers = data.get("Customers", [])
                if not customers:
                    print(f"✅ All customers pulled. Stopping on page {page}.")
                    break

                all_customers.extend(customers)
                print(f"📄 Pulled page {page} — {len(customers)} customers")
                
                # Check if we have reached the end
                current_end = data.get("ResultStatistics", {}).get("EndRecord", 0)
                total_records = data.get("ResultStatistics", {}).get("TotalRecords", 0)

                if current_end >= total_records:
                    print(f"✅ Reached the end of available customer data (Total: {total_records}).")
                    break

                page += 1
                break  # Exit retry loop on success

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    jitter = random.uniform(0.5, 1.5)
                    wait_time = retry_delay * (2 ** (attempt - 1)) * jitter
                    print(f"⚠️ Rate limited (attempt {attempt}/{max_retries}). Retrying in {wait_time:.2f} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ RICS API request failed on page {page}: {e}")
                    raise
        else:
            print(f"❌ Max retries reached on page {page}. Exiting.")
            raise Exception("Max retries reached. Aborting.")

    # Save results to CSV
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_dir = "./output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/rics_customers_{timestamp}.csv"

    try:
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

        print(f"✅ Successfully saved RICS customer data to {output_path}")

    except Exception as e:
        print(f"❌ Failed to save RICS data: {e}")

    print("✅ Data export completed.")


fetch_rics_data()
