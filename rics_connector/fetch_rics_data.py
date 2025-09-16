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
import json

# --- CONFIGURATION ---
TEST_MODE = False
MAX_SKIP = 10000
MAX_CUSTOMERS = None
MAX_PURCHASE_PAGES = None
MAX_WORKERS = 1   # safer for RICS rate limits
DEBUG_MODE = False

MAX_RETRIES = 3
ABSOLUTE_TIMEOUT_SECONDS = 120

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
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %I:%M:%S %p",
    ):
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    return None

def fetch_purchase_history_for_customer(cust_id, customer_info, max_purchase_pages=None, debug_mode=False):
    ph_skip, ph_take, page_count = 0, 100, 0
    all_rows = []

    start_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    payload_variants = [
        {"CustomerId": cust_id, "Take": ph_take, "Skip": ph_skip,
         "StartDate": start_date, "EndDate": end_date},
        {"CustomerId": cust_id, "Take": ph_take, "Skip": ph_skip,
         "StartTicketDate": start_date, "EndTicketDate": end_date}
    ]

    while True:
        sale_headers = []
        ph_data = {}

        for payload in payload_variants:
            for attempt in range(MAX_RETRIES):
                try:
                    log_message(f"[START] Purchases for cust {cust_id}, skip {ph_skip}, page {page_count+1}")
                    log_message(f"📤 Payload: {json.dumps(payload)}")

                    resp = requests.post(
                        "https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory",
                        headers={"Token": RICS_API_TOKEN},
                        json=payload,
                        timeout=ABSOLUTE_TIMEOUT_SECONDS
                    )
                    resp.raise_for_status()

                    if debug_mode and page_count == 0 and ph_skip == 0:
                        log_message(f"📥 Raw response: {resp.text}")

                    ph_data = resp.json()
                    if not ph_data.get("IsSuccessful"):
                        log_message(f"⚠️ Not successful for {cust_id}: {ph_data}")
                        continue

                    sale_headers = ph_data.get("SaleHeaders", [])
                    break
                except requests.exceptions.HTTPError as e:
                    if resp.status_code == 429 and attempt < MAX_RETRIES - 1:
                        wait = [1, 5, 15][attempt]
                        log_message(f"⚠️ 429 rate limit. Retrying in {wait}s...")
                        time.sleep(wait)
                        continue
                    log_message(f"❌ HTTP error for {cust_id}: {e}")
                except Exception as e:
                    log_message(f"❌ Error fetching purchases for {cust_id}: {e}")
            if sale_headers:
                break

        if not sale_headers:
            break

        for sale in sale_headers:
            log_message(f"🔑 Sale keys: {list(sale.keys())}")

            sale_dt = parse_dt(sale.get("SaleDateTime") or sale.get("TicketDateTime"))
            if not sale_dt:
                log_message(f"⚠️ Skipping sale (no date): {sale}")
                continue
            if sale_dt < CUTOFF_DATE:
                log_message(f"⏩ Skipped old sale ({sale_dt}) for {cust_id}")
                continue

            sale_info = {k: sale.get(k) for k in [
                "TicketDateTime","TicketNumber","Change","TicketVoided",
                "ReceiptPrinted","TicketSuspended","ReceiptEmailed",
                "SaleDateTime","TicketModifiedOn","ModifiedBy","CreatedOn"
            ]}

            for item in sale.get("CustomerPurchases", []):
                log_message(f"🔑 Item keys: {list(item.keys())}")
                item_info = {k: item.get(k) for k in [
                    "TicketLineNumber","Quantity","AmountPaid","Sku","Summary",
                    "Description","SupplierCode","SupplierName","Color","Column",
                    "Row","OnHand"
                ]}
                row = {**customer_info, **sale_info, **item_info}
                log_message(f"📝 ROW PREVIEW: {row}")
                all_rows.append(row)

        result_stats = ph_data.get("ResultStatistics", {})
        end_record = result_stats.get("EndRecord", 0)
        total_records = result_stats.get("TotalRecords", 0)
        page_count += 1
        if (max_purchase_pages and page_count >= max_purchase_pages) or end_record >= total_records:
            break
        ph_skip += ph_take

    log_message(f"📦 Cust {cust_id}: {len(all_rows)} rows (7d), {page_count} pages")
    return all_rows

def fetch_rics_data_with_purchase_history(max_customers=None, max_purchase_pages=None, debug_mode=False):
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_customer_purchase_history_{timestamp}.csv"
    output_dir = os.path.join("optimizely_connector", "output")
    output_path = os.path.join(output_dir, filename)
    os.makedirs(output_dir, exist_ok=True)

    all_rows, customer_infos = [], []
    total_customers = 0

    STORE_CODES = [12132, 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99]

    for store_code in STORE_CODES:
        skip, store_customers = 0, 0
        log_message(f"🏪 Store {store_code} starting")

        while skip < MAX_SKIP:
            try:
                resp = requests.post(
                    "https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                    headers={"Token": RICS_API_TOKEN},
                    json={"StoreCode": store_code, "Skip": skip, "Take": 100},
                    timeout=ABSOLUTE_TIMEOUT_SECONDS
                )
                resp.raise_for_status()
                if debug_mode and skip == 0:
                    log_message(f"🔍 DEBUG customer resp (store {store_code}): {resp.text[:500]}")
                customers = resp.json().get("Customers", [])
            except Exception as e:
                log_message(f"❌ Error fetching customers from store {store_code}: {e}")
                break

            if not customers:
                log_message(f"⚠️ No customers returned for store {store_code}, skip {skip}")
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

        log_message(f"✅ Store {store_code} finished → {store_customers} customers queued")

    log_message(f"🧮 Total customers queued: {len(customer_infos)}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                fetch_purchase_history_for_customer, cid, info, max_purchase_pages, debug_mode
            ): (cid, info)
            for cid, info in customer_infos
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                rows = future.result()
                all_rows.extend(rows)
            except Exception as exc:
                log_message(f"❌ Error in thread: {exc}")

    log_message(f"📊 About to write {len(all_rows)} rows to {output_path}")
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
        MAX_SKIP, MAX_CUSTOMERS, MAX_PURCHASE_PAGES, MAX_WORKERS = 3, 3, 1, 1
    if args.max_customers:
        MAX_CUSTOMERS = args.max_customers
    if args.max_purchase_pages:
        MAX_PURCHASE_PAGES = args.max_purchase_pages
    if args.debug:
        DEBUG_MODE = True

    fetch_rics_data_with_purchase_history(MAX_CUSTOMERS, MAX_PURCHASE_PAGES, DEBUG_MODE)
