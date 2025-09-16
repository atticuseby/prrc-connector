# scripts/debug_rics_pull.py

import os
import requests
from datetime import datetime

RICS_API_TOKEN = os.getenv("RICS_API_TOKEN")

if not RICS_API_TOKEN:
    raise RuntimeError("❌ No RICS_API_TOKEN found in env vars!")

CUSTOMER_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomer"
PURCHASE_URL = "https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory"

def debug_rics_pull():
    print(f"[{datetime.now()}] 🔍 Starting RICS debug pull")
    headers = {"Token": RICS_API_TOKEN}

    # --- STEP 1: Get first 5 customers ---
    resp = requests.post(
        CUSTOMER_URL,
        headers=headers,
        json={"StoreCode": 12132, "Skip": 0, "Take": 5},
        timeout=60
    )
    data = resp.json()
    print("\n=== RAW CUSTOMER RESPONSE ===")
    print(data)

    customers = data.get("Customers", [])
    if not customers:
        print("⚠️ No customers returned. Exiting early.")
        return

    first_customer = customers[0]
    cust_id = first_customer.get("CustomerId")
    print(f"\n➡️ Testing purchase history for CustomerId={cust_id}")

    # --- STEP 2: Get first purchase history page ---
    resp2 = requests.post(
        PURCHASE_URL,
        headers=headers,
        json={"CustomerId": cust_id, "Skip": 0, "Take": 5},
        timeout=60
    )
    ph_data = resp2.json()
    print("\n=== RAW PURCHASE HISTORY RESPONSE ===")
    print(ph_data)

    print("\n✅ Debug run complete. No CSV written.")

if __name__ == "__main__":
    debug_rics_pull()
