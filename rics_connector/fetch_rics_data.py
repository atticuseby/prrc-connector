import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import csv
import time
import requests
from datetime import datetime
from scripts.config import RICS_API_TOKEN, TEST_EMAIL
from scripts.helpers import log_message
import concurrent.futures
import argparse

# --- CONFIGURABLE TESTING/OPTIMIZATION FLAGS ---
TEST_MODE = True  # Set to False for full run
MAX_SKIP = 3 if TEST_MODE else 10000  # Lowered for quick test
MAX_CUSTOMERS = 3 if TEST_MODE else None  # Default, can be overridden by CLI
MAX_PURCHASE_PAGES = 1 if TEST_MODE else None  # Default, can be overridden by CLI
MAX_WORKERS = 10 if TEST_MODE else 3  # Increase parallelism for test
DEBUG_MODE = False  # Set by CLI
# ----------------------------------------------

MAX_RETRIES = 3
MAX_RESPONSE_TIME_SECONDS = 60  # Only warn if exceeded — don’t abort
ABSOLUTE_TIMEOUT_SECONDS = 120  # This ensures the request doesn’t hang forever

# Fields for the detailed purchase history CSV
purchase_history_fields = [
    # Customer info
    "rics_id", "email", "first_name", "last_name", "orders", "total_spent", "city", "state", "zip", "phone",
    # Sale header info
    "TicketDateTime", "TicketNumber", "Change", "TicketVoided", "ReceiptPrinted", "TicketSuspended", "ReceiptEmailed", "SaleDateTime", "TicketModifiedOn", "ModifiedBy", "CreatedOn",
    # Line item info
    "TicketLineNumber", "Quantity", "AmountPaid", "Sku", "Summary", "Description", "SupplierCode", "SupplierName", "Color", "Column", "Row", "OnHand"
]

def fetch_purchase_history_for_customer(cust_id, customer_info, max_purchase_pages=None, debug_mode=False):
    ph_skip = 0
    ph_take = 100
    all_rows = []
    page_count = 0
    api_calls = 0
    while True:
        # Try both 'Token' and 'token' headers for compatibility
        ph_headers_variants = [
            {"Token": RICS_API_TOKEN},
            {"token": RICS_API_TOKEN}
        ]
        sale_headers = []
        for ph_headers in ph_headers_variants:
            try:
                start_time = time.time()
                log_message(f"[START] {datetime.now()} - Fetching purchase history for customer {cust_id} with header {list(ph_headers.keys())[0]}, skip {ph_skip}, page {page_count+1}")
                ph_response = requests.post(
                    url="https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory",
                    headers=ph_headers,
                    json={"CustomerId": cust_id, "Take": ph_take, "Skip": ph_skip},
                    timeout=ABSOLUTE_TIMEOUT_SECONDS
                )
                api_calls += 1
                duration = time.time() - start_time
                log_message(f"[END] {datetime.now()} - Purchase history API call took {duration:.2f}s for customer {cust_id}, page {page_count+1}")
                ph_response.raise_for_status()
                ph_data = ph_response.json()
                if not ph_data.get("IsSuccessful"):
                    log_message(f"⚠️ Purchase history failed for customer {cust_id} with header {list(ph_headers.keys())[0]}: {ph_data.get('Message')}")
                    continue
                sale_headers = ph_data.get("SaleHeaders", [])
                log_message(f"✅ Used header for purchase history: {list(ph_headers.keys())[0]}")
                break
            except requests.exceptions.HTTPError as e:
                if ph_response.status_code == 401:
                    log_message(f"❌ 401 Unauthorized for purchase history with header: {list(ph_headers.keys())[0]}")
                    continue
                else:
                    log_message(f"❌ HTTP error for purchase history with header {list(ph_headers.keys())[0]}: {e}")
                    break
            except Exception as e:
                log_message(f"❌ Error fetching purchase history for customer {cust_id} with header {list(ph_headers.keys())[0]}: {e}")
                continue
        if not sale_headers:
            break
        for sale in sale_headers:
            sale_info = {
                "TicketDateTime": sale.get("TicketDateTime"),
                "TicketNumber": sale.get("TicketNumber"),
                "Change": sale.get("Change"),
                "TicketVoided": sale.get("TicketVoided"),
                "ReceiptPrinted": sale.get("ReceiptPrinted"),
                "TicketSuspended": sale.get("TicketSuspended"),
                "ReceiptEmailed": sale.get("ReceiptEmailed"),
                "SaleDateTime": sale.get("SaleDateTime"),
                "TicketModifiedOn": sale.get("TicketModifiedOn"),
                "ModifiedBy": sale.get("ModifiedBy"),
                "CreatedOn": sale.get("CreatedOn")
            }
            for item in sale.get("CustomerPurchases", []):
                item_info = {
                    "TicketLineNumber": item.get("TicketLineNumber"),
                    "Quantity": item.get("Quantity"),
                    "AmountPaid": item.get("AmountPaid"),
                    "Sku": item.get("Sku"),
                    "Summary": item.get("Summary"),
                    "Description": item.get("Description"),
                    "SupplierCode": item.get("SupplierCode"),
                    "SupplierName": item.get("SupplierName"),
                    "Color": item.get("Color"),
                    "Column": item.get("Column"),
                    "Row": item.get("Row"),
                    "OnHand": item.get("OnHand")
                }
                row = {**customer_info, **sale_info, **item_info}
                all_rows.append(row)
        # Pagination
        result_stats = ph_data.get("ResultStatistics", {})
        end_record = result_stats.get("EndRecord", 0)
        total_records = result_stats.get("TotalRecords", 0)
        page_count += 1
        if (max_purchase_pages is not None and page_count >= max_purchase_pages) or end_record >= total_records:
            break
        ph_skip += ph_take
        if debug_mode:
            log_message(f"[DEBUG] Early exit after first purchase history page for customer {cust_id}")
            break
    log_message(f"📦 Customer {cust_id} purchase history: {len(all_rows)} rows, {page_count} pages, {api_calls} API calls")
    return all_rows


def fetch_rics_data_with_purchase_history(max_customers=None, max_purchase_pages=None, debug_mode=False):
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_customer_purchase_history_{timestamp}.csv"
    output_path = os.path.join("data", filename)

    os.makedirs("data", exist_ok=True)

    all_rows = []
    skip = 0
    customer_infos = []
    total_api_calls = 0
    total_customers = 0
    fetch_start = time.time()

    while skip < MAX_SKIP:
        log_message(f"\n📄 Requesting customers from skip: {skip}...")
        attempt = 0
        headers_variants = [
            {"Token": RICS_API_TOKEN},
            {"token": RICS_API_TOKEN}
        ]
        customers = []
        for headers in headers_variants:
            try:
                start = time.time()
                log_message(f"[START] {datetime.now()} - Fetching customers with header {list(headers.keys())[0]}, skip {skip}")
                response = requests.post(
                    url="https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                    headers=headers,
                    json={"StoreCode": 12132, "Skip": skip, "Take": 100},
                    timeout=ABSOLUTE_TIMEOUT_SECONDS
                )
                total_api_calls += 1
                duration = time.time() - start
                log_message(f"[END] {datetime.now()} - Customer API call took {duration:.2f}s with headers: {list(headers.keys())[0]}")
                response.raise_for_status()
                customers = response.json().get("Customers", [])
                log_message(f"✅ Used header: {list(headers.keys())[0]}")
                break
            except requests.exceptions.HTTPError as e:
                if response.status_code == 401:
                    log_message(f"❌ 401 Unauthorized with header: {list(headers.keys())[0]}")
                    continue
                else:
                    log_message(f"❌ HTTP error with header {list(headers.keys())[0]}: {e}")
                    break
            except Exception as e:
                log_message(f"❌ Attempt {attempt+1} failed with header {list(headers.keys())[0]}: {e}")
                continue
        if not customers:
            attempt += 1
            time.sleep(3)
            continue
        if duration > MAX_RESPONSE_TIME_SECONDS:
            log_message(f"⚠️ Warning: Response exceeded {MAX_RESPONSE_TIME_SECONDS}s, but continuing anyway.")
            break  # Success

        for idx, customer in enumerate(customers):
            mailing = customer.get("MailingAddress", {})
            customer_info = {
                "rics_id": customer.get("CustomerId"),
                "email": customer.get("Email", "").strip(),
                "first_name": customer.get("FirstName", "").strip(),
                "last_name": customer.get("LastName", "").strip(),
                "orders": customer.get("OrderCount", 0),
                "total_spent": customer.get("TotalSpent", 0),
                "city": mailing.get("City", "").strip(),
                "state": mailing.get("State", "").strip(),
                "zip": mailing.get("PostalCode", "").strip(),
                "phone": customer.get("PhoneNumber", "").strip()
            }
            cust_id = customer.get("CustomerId")
            if not cust_id:
                continue
            log_message(f"➡️ [{skip+idx+1}] Queuing purchase history fetch for customer {cust_id} ({customer_info['email']})")
            customer_infos.append((cust_id, customer_info))
            total_customers += 1
            if max_customers is not None and total_customers >= max_customers:
                break
        if max_customers is not None and total_customers >= max_customers:
            break
        skip += 100

    log_message(f"🧮 Total customers queued: {len(customer_infos)}")
    # Parallelize purchase history fetches
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_cust = {executor.submit(fetch_purchase_history_for_customer, cust_id, info, max_purchase_pages, debug_mode): (cust_id, info) for cust_id, info in customer_infos}
        for i, future in enumerate(concurrent.futures.as_completed(future_to_cust)):
            cust_id, info = future_to_cust[future]
            try:
                rows = future.result()
                log_message(f"✅ [Thread] Finished purchase history for customer {cust_id} ({info['email']}) with {len(rows)} rows")
                all_rows.extend(rows)
                if debug_mode:
                    log_message(f"[DEBUG] Early exit after first customer (debug mode)")
                    break
            except Exception as exc:
                log_message(f"❌ [Thread] Error for customer {cust_id} ({info['email']}): {exc}")
            if debug_mode:
                break
        if debug_mode:
            log_message(f"[DEBUG] Early exit after first customer (debug mode) - outside thread loop")
            # Only process the first customer in debug mode
            pass

    # Write to CSV
    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=purchase_history_fields)
        writer.writeheader()
        writer.writerows(all_rows)
    elapsed = time.time() - fetch_start
    log_message(f"📝 Wrote purchase history CSV to: {output_path}")
    log_message(f"⏱️ Total elapsed time: {elapsed:.2f}s")
    log_message(f"🔢 Total customer API calls: {total_api_calls}")
    log_message(f"🔢 Total customers processed: {len(customer_infos)}")
    log_message(f"🔢 Total purchase history rows: {len(all_rows)}")

    return output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch RICS data with purchase history.")
    parser.add_argument('--test', action='store_true', help='Run in test mode (limits data, increases parallelism)')
    parser.add_argument('--max-customers', type=int, default=None, help='Maximum number of customers to fetch (overrides test mode default)')
    parser.add_argument('--max-purchase-pages', type=int, default=None, help='Maximum purchase history pages per customer (overrides test mode default)')
    parser.add_argument('--debug', action='store_true', help='Debug mode: only fetch first customer and first purchase history page, then exit')
    args = parser.parse_args()
    if args.test:
        TEST_MODE = True
    if args.max_customers is not None:
        MAX_CUSTOMERS = args.max_customers
    else:
        MAX_CUSTOMERS = 3 if TEST_MODE else None
    if args.max_purchase_pages is not None:
        MAX_PURCHASE_PAGES = args.max_purchase_pages
    else:
        MAX_PURCHASE_PAGES = 1 if TEST_MODE else None
    if args.debug:
        DEBUG_MODE = True
    fetch_rics_data_with_purchase_history(max_customers=MAX_CUSTOMERS, max_purchase_pages=MAX_PURCHASE_PAGES, debug_mode=DEBUG_MODE)
