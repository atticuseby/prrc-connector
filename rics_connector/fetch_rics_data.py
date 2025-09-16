import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import csv
import time
import requests
from datetime import datetime, timedelta
from scripts.config import RICS_API_TOKEN, TEST_EMAIL
from scripts.helpers import log_message
import concurrent.futures
import argparse

# --- CONFIGURATION ---
TEST_MODE = False  # default off for production
MAX_SKIP = 10000   # pagination limit
MAX_CUSTOMERS = None
MAX_PURCHASE_PAGES = None
MAX_WORKERS = 3
DEBUG_MODE = False

MAX_RETRIES = 3
MAX_RESPONSE_TIME_SECONDS = 60
ABSOLUTE_TIMEOUT_SECONDS = 120

# Only include purchases within the last 7 days
CUTOFF_DATE = datetime.utcnow() - timedelta(days=7)

purchase_history_fields = [
    "rics_id", "email", "first_name", "last_name", "orders", "total_spent",
    "city", "state", "zip", "phone",
    "TicketDateTime", "TicketNumber", "Change", "TicketVoided", "ReceiptPrinted",
    "TicketSuspended", "ReceiptEmailed", "SaleDateTime", "TicketModifiedOn",
    "ModifiedBy", "CreatedOn",
    "TicketLineNumber", "Quantity", "AmountPaid", "Sku", "Summary",
    "Description", "SupplierCode", "SupplierName", "Color", "Column", "Row", "OnHand"
]

def parse_dt(dt_str):
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"):
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    return None

def fetch_purchase_history_for_customer(cust_id, customer_info, max_purchase_pages=None, debug_mode=False):
    ph_skip, ph_take, page_count, api_calls = 0, 100, 0, 0
    all_rows = []

    # Build a potential date filter payload for the API
    start_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        ph_headers_variants = [{"Token": RICS_API_TOKEN}, {"token": RICS_API_TOKEN}]
        sale_headers = []
        ph_data = {}

        for ph_headers in ph_headers_variants:
            try:
                start_time = time.time()
                log_message(f"[START] Fetching purchases for {cust_id}, skip {ph_skip}, page {page_count+1}")

                resp = requests.post(
                    "https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory",
                    headers=ph_headers,
                    json={
                        "CustomerId": cust_id,
                        "Take": ph_take,
                        "Skip": ph_skip,
                        # 🚨 If RICS supports these fields, it will respect them
                        "StartDate": start_date,
                        "EndDate": end_date
                    },
                    timeout=ABSOLUTE_TIMEOUT_SECONDS
                )

                api_calls += 1
                resp.raise_for_status()
                ph_data = resp.json()
                if not ph_data.get("IsSuccessful"):
                    continue
                sale_headers = ph_data.get("SaleHeaders", [])
                break
            except Exception as e:
                log_message(f"❌ Error fetching purchases for {cust_id}: {e}")
                continue

        if not sale_headers:
            break

        for sale in sale_headers:
            # Local safeguard — only last 7 days
            sale_dt = parse_dt(sale.get("SaleDateTime") or sale.get("TicketDateTime"))
            if not sale_dt or sale_dt < CUTOFF_DATE:
                continue

            sale_info = {k: sale.get(k) for k in [
                "TicketDateTime","TicketNumber","Change","TicketVoided",
                "ReceiptPrinted","TicketSuspended","ReceiptEmailed",
                "SaleDateTime","TicketModifiedOn","ModifiedBy","CreatedOn"
            ]}

            for item in sale.get("CustomerPurchases", []):
                item_info = {k: item.get(k) for k in [
                    "TicketLineNumber","Quantity","AmountPaid","Sku","Summary",
                    "Description","SupplierCode","SupplierName","Color","Column",
                    "Row","OnHand"
                ]}
                row = {**customer_info, **sale_info, **item_info}
                all_rows.append(row)

        result_stats = ph_data.get("ResultStatistics", {})
        end_record = result_stats.get("EndRecord", 0)
        total_records = result_stats.get("TotalRecords", 0)
        page_count += 1
        if (max_purchase_pages and page_count >= max_purchase_pages) or end_record >= total_records:
            break
        ph_skip += ph_take
        if debug_mode:
            break

    log_message(f"📦 Customer {cust_id}: {len(all_rows)} rows (last 7 days), {page_count} pages, {api_calls} calls")
    return all_rows

def fetch_rics_data_with_purchase_history(max_customers=None, max_purchase_pages=None, debug_mode=False):
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_customer_purchase_history_{timestamp}.csv"
    output_dir = os.path.join("optimizely_connector", "output")
    output_path = os.path.join(output_dir, filename)
    os.makedirs(output_dir, exist_ok=True)

    all_rows, skip, customer_infos = [], 0, []
    total_api_calls, total_customers = 0, 0

# Parent must go first, followed by all active child store codes
STORE_CODES = [12132, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99]

for store_code in STORE_CODES:
    skip = 0
    store_customers = 0
    log_message(f"🏪 Starting fetch for Store {store_code}")

    while skip < MAX_SKIP:
        headers_variants = [{"Token": RICS_API_TOKEN}, {"token": RICS_API_TOKEN}]
        customers = []

        for headers in headers_variants:
            try:
                resp = requests.post(
                    "https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                    headers=headers,
                    json={"StoreCode": store_code, "Skip": skip, "Take": 100},
                    timeout=ABSOLUTE_TIMEOUT_SECONDS
                )
                total_api_calls += 1
                resp.raise_for_status()
                customers = resp.json().get("Customers", [])
                break
            except Exception as e:
                log_message(f"❌ Error fetching customers from store {store_code}: {e}")
                continue

        if not customers:
            break

        for customer in customers:
            mailing = customer.get("MailingAddress", {})
            info = {
                "rics_id": customer.get("CustomerId"),
                "email": (customer.get("Email") or "").strip(),
                "first_name": (customer.get("FirstName") or "").strip(),
                "last_name": (customer.get("LastName") or "").strip(),
                "orders": customer.get("OrderCount", 0),
                "total_spent": customer.get("TotalSpent", 0),
                "city": mailing.get("City", "").strip(),
                "state": mailing.get("State", "").strip(),
                "zip": mailing.get("PostalCode", "").strip(),
                "phone": (customer.get("PhoneNumber") or "").strip()
            }
            cust_id = customer.get("CustomerId")
            if cust_id:
                customer_infos.append((cust_id, info))
                store_customers += 1
                total_customers += 1
                if max_customers and total_customers >= max_customers:
                    break

        if max_customers and total_customers >= max_customers:
            break
        skip += 100

    log_message(f"✅ Finished Store {store_code} → {store_customers} customers queued")
    
        for customer in customers:
            mailing = customer.get("MailingAddress", {})
            info = {
                "rics_id": customer.get("CustomerId"),
                "email": (customer.get("Email") or "").strip(),
                "first_name": (customer.get("FirstName") or "").strip(),
                "last_name": (customer.get("LastName") or "").strip(),
                "orders": customer.get("OrderCount", 0),
                "total_spent": customer.get("TotalSpent", 0),
                "city": mailing.get("City", "").strip(),
                "state": mailing.get("State", "").strip(),
                "zip": mailing.get("PostalCode", "").strip(),
                "phone": (customer.get("PhoneNumber") or "").strip()
            }
            cust_id = customer.get("CustomerId")
            if cust_id:
                customer_infos.append((cust_id, info))
                total_customers += 1
                if max_customers and total_customers >= max_customers:
                    break

        if max_customers and total_customers >= max_customers:
            break
        skip += 100

        for customer in customers:
            mailing = customer.get("MailingAddress", {})
            info = {
                "rics_id": customer.get("CustomerId"),
                "email": (customer.get("Email") or "").strip(),
                "first_name": (customer.get("FirstName") or "").strip(),
                "last_name": (customer.get("LastName") or "").strip(),
                "orders": customer.get("OrderCount", 0),
                "total_spent": customer.get("TotalSpent", 0),
                "city": mailing.get("City", "").strip(),
                "state": mailing.get("State", "").strip(),
                "zip": mailing.get("PostalCode", "").strip(),
                "phone": (customer.get("PhoneNumber") or "").strip()
            }
            cust_id = customer.get("CustomerId")
            if cust_id:
                customer_infos.append((cust_id, info))
                total_customers += 1
                if max_customers and total_customers >= max_customers:
                    break
        if max_customers and total_customers >= max_customers:
            break
        skip += 100

    log_message(f"🧮 Total customers queued: {len(customer_infos)}")
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_purchase_history_for_customer, cid, info, max_purchase_pages, debug_mode): (cid, info) for cid, info in customer_infos}
        for future in concurrent.futures.as_completed(futures):
            try:
                rows = future.result()
                all_rows.extend(rows)
                if debug_mode:
                    break
            except Exception as exc:
                log_message(f"❌ Error in thread: {exc}")

    with open(output_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=purchase_history_fields)
        writer.writeheader()
        writer.writerows(all_rows)

    log_message(f"📝 Wrote {len(all_rows)} rows to {output_path}")
    return output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch RICS data with purchase history.")
    parser.add_argument('--test', action='store_true', help='Enable test mode (limits customers and pages)')
    parser.add_argument('--max-customers', type=int, help='Override max customers')
    parser.add_argument('--max-purchase-pages', type=int, help='Override max purchase pages')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    args = parser.parse_args()

    if args.test:
        TEST_MODE = True
        MAX_SKIP, MAX_CUSTOMERS, MAX_PURCHASE_PAGES, MAX_WORKERS = 3, 3, 1, 10
    if args.max_customers:
        MAX_CUSTOMERS = args.max_customers
    if args.max_purchase_pages:
        MAX_PURCHASE_PAGES = args.max_purchase_pages
    if args.debug:
        DEBUG_MODE = True

    fetch_rics_data_with_purchase_history(MAX_CUSTOMERS, MAX_PURCHASE_PAGES, DEBUG_MODE)
