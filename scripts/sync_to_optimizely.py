import os
import csv
import requests
import time
from datetime import datetime

OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN", "").strip()
ODP_API_URL = "https://api.customer.io/v1/customers"
LOG_FILE = "optimizely_connector/output/optimizely_sync_log.csv"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Ensure log directory exists
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

def has_already_synced(email):
    if not os.path.exists(LOG_FILE):
        return False
    with open(LOG_FILE, "r") as f:
        for line in f:
            if email in line:
                return True
    return False

def log_success(email, status):
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now().isoformat()},{email},{status}\n")

def send_to_optimizely(payload):
    if not OPTIMIZELY_API_TOKEN:
        print("❌ Missing OPTIMIZELY_API_TOKEN — cannot send to Optimizely")
        return

    email = payload["identifiers"].get("email")
    if not email:
        print("⚠️ Skipping: No email provided in payload.")
        return

    if has_already_synced(email):
        print(f"⏭️ Skipping duplicate: {email}")
        return

    headers = {
        "Authorization": f"Bearer {OPTIMIZELY_API_TOKEN}",
        "Content-Type": "application/json"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.post(ODP_API_URL, json=payload, headers=headers)
            if response.status_code == 202:
                print(f"✅ [Optimizely] Synced: {email}")
                log_success(email, "202")
                return
            else:
                print(f"❌ [Attempt {attempt}] Failed {response.status_code}: {response.text}")
                if response.status_code >= 500 or response.status_code == 429:
                    time.sleep(RETRY_DELAY * attempt)
                    continue
                else:
                    break
        except Exception as e:
            print(f"❌ [Attempt {attempt}] Exception: {e}")
            time.sleep(RETRY_DELAY * attempt)

    log_success(email, f"Failed after {MAX_RETRIES}")

def main(input_file):
    if not os.path.exists(input_file):
        print(f"❌ Input file not found: {input_file}")
        return

    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row.get("CustomerId") or row.get("Email")
            if not email:
                continue

            payload = {
                "identifiers": {"email": email},
                "attributes": {
                    "storeCode": row.get("StoreCode"),
                    "transactionId": row.get("TicketNumber"),
                    "amountPaid": row.get("AmountPaid"),
                    "transactionDate": row.get("TicketDateTime")
                }
            }
            send_to_optimizely(payload)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python sync_to_optimizely.py <deduped_csv>")
        sys.exit(1)
    main(sys.argv[1])
