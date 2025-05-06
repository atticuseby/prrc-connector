# fetch_rics_data.py

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
    page = 1
    page_size = 1000
    total_records = None

    print("🔍 Starting paginated RICS customer sync...")

    while True:
        payload = {
            "DateOfBirthStart": "1950-01-01",
            "DateOfBirthEnd": "2025-12-31",
            "Page": page,
            "PageSize": page_size
        }

        try:
            response = requests.post(RICS_API_URL, headers=headers, json=payload)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            log_message(f"❌ RICS API request failed on page {page}: {e}")
            raise

        data = response.json()

        if not data.get("IsSuccessful", False):
            log_message(f"❌ RICS API returned failure on page {page}")
            raise Exception("RICS API returned unsuccessful status")

        customers = data.get("Customers", [])
        all_customers.extend(customers)

        if total_records is None:
            total_records = data.get("ResultStatistics", {}).get("TotalRecords", 0)

        current_end = data.get("ResultStatistics", {}).get("EndRecord", 0)
        print(f"📄 Pulled page {page} — {len(customers)} customers")

        if current_end >= total_records:
            break

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

    # Optional: S3 upload (disabled for now)
    # upload_to_s3(output_path, bucket_name="prrc-daily-backups")

# Uncomment and configure this when you're ready for S3 uploads
# def upload_to_s3(filepath, bucket_name):
#     import boto3
#     from botocore.exceptions import NoCredentialsError
#     s3 = boto3.client("s3")
#     try:
#         s3.upload_file(filepath, bucket_name, os.path.basename(filepath))
#         log_message(f"☁️ Uploaded {os.path.basename(filepath)} to {bucket_name}")
#     except NoCredentialsError:
#         log_message("❌ S3 upload failed: No AWS credentials found")
