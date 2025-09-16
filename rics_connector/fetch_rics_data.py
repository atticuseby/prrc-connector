import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
import json
import argparse
import time
from datetime import datetime, timedelta
from scripts.config import RICS_API_TOKEN
from scripts.helpers import log_message

ABSOLUTE_TIMEOUT_SECONDS = 120

def fetch_full_purchase_history_for_customer(cust_id):
    """
    Diagnostic: pull ALL purchase history for a single known customer.
    - No cutoff date
    - Dumps raw JSON response
    """
    log_message(f"🔍 Starting diagnostic purchase fetch for CustomerId={cust_id}")

    payload_variants = [
        {"CustomerId": cust_id, "Take": 100, "Skip": 0},  # no dates
        {"CustomerId": cust_id, "Take": 100, "Skip": 0,
         "StartDate": "2000-01-01T00:00:00Z", "EndDate": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")},
        {"CustomerId": cust_id, "Take": 100, "Skip": 0,
         "StartTicketDate": "2000-01-01T00:00:00Z", "EndTicketDate": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")}
    ]

    for i, payload in enumerate(payload_variants, 1):
        try:
            log_message(f"📤 Payload {i}: {json.dumps(payload)}")
            resp = requests.post(
                "https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory",
                headers={"Token": RICS_API_TOKEN},
                json=payload,
                timeout=ABSOLUTE_TIMEOUT_SECONDS
            )
            resp.raise_for_status()

            raw_text = resp.text
            log_message(f"📥 Raw response (payload {i}, first 1000 chars): {raw_text[:1000]}")

            try:
                parsed = resp.json()
                log_message(f"✅ Parsed keys: {list(parsed.keys())}")
                if "SaleHeaders" in parsed:
                    log_message(f"📦 Found {len(parsed['SaleHeaders'])} SaleHeaders")
                    for sale in parsed["SaleHeaders"][:5]:  # only show first 5
                        log_message(f"🧾 Sample SaleHeader: {json.dumps(sale, indent=2)[:500]}")
            except Exception as e:
                log_message(f"⚠️ Could not parse JSON: {e}")

        except Exception as e:
            log_message(f"❌ Error with payload {i}: {e}")
        time.sleep(1)  # short pause between payloads

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Diagnostic: Fetch full purchase history for a single customer")
    parser.add_argument("--customer-id", required=True, help="RICS CustomerId (GUID) to fetch")
    args = parser.parse_args()

    fetch_full_purchase_history_for_customer(args.customer_id)
