import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import requests
import json
import argparse
import time
from datetime import datetime
from scripts.config import RICS_API_TOKEN
from scripts.helpers import log_message

ABSOLUTE_TIMEOUT_SECONDS = 120

def resolve_customer_guid(account_id, store_code=1):
    """
    Resolve an AccountId (from RICS reports) to a CustomerId (GUID) via GetCustomer.
    """
    payload = {
        "StoreCode": store_code,
        "AccountId": account_id,
        "Take": 1,
        "Skip": 0
    }
    try:
        log_message(f"📤 Resolving AccountId {account_id} in Store {store_code} → {json.dumps(payload)}")
        resp = requests.post(
            "https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
            headers={"Token": RICS_API_TOKEN},
            json=payload,
            timeout=ABSOLUTE_TIMEOUT_SECONDS
        )
        resp.raise_for_status()
        data = resp.json()
        customers = data.get("Customers", [])
        if not customers:
            log_message(f"⚠️ No customers found for AccountId {account_id} in store {store_code}")
            return None
        customer = customers[0]
        log_message(f"✅ Resolved AccountId {account_id} → CustomerId {customer.get('CustomerId')}")
        return customer.get("CustomerId")
    except Exception as e:
        log_message(f"❌ Error resolving AccountId {account_id}: {e}")
        return None

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
                    for sale in parsed["SaleHeaders"][:5]:  # show first 5
                        log_message(f"🧾 Sample SaleHeader: {json.dumps(sale, indent=2)[:500]}")
            except Exception as e:
                log_message(f"⚠️ Could not parse JSON: {e}")

        except Exception as e:
            log_message(f"❌ Error with payload {i}: {e}")
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Diagnostic: Fetch full purchase history for a single customer")
    parser.add_argument("--customer-id", help="RICS CustomerId (GUID) to fetch")
    parser.add_argument("--account-id", help="RICS AccountId (from Export Purchase Details report)")
    parser.add_argument("--store-code", type=int, default=1, help="Store code for AccountId resolution (default=1)")
    args = parser.parse_args()

    cust_id = args.customer_id
    if args.account_id and not cust_id:
        cust_id = resolve_customer_guid(args.account_id, args.store_code)

    if not cust_id:
        log_message("❌ No valid CustomerId provided or resolved. Exiting.")
        sys.exit(1)

    fetch_full_purchase_history_for_customer(cust_id)
