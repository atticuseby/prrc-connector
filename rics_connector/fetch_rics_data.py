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

# --- CONFIGURABLE TESTING/OPTIMIZATION FLAGS ---
TEST_MODE = True  # Set to False for full run
MAX_SKIP = 3 if TEST_MODE else 10000
MAX_CUSTOMERS = 3 if TEST_MODE else None
MAX_PURCHASE_PAGES = 1 if TEST_MODE else None
MAX_WORKERS = 10 if TEST_MODE else 3
DEBUG_MODE = False
# ----------------------------------------------

MAX_RETRIES = 3
MAX_RESPONSE_TIME_SECONDS = 60
ABSOLUTE_TIMEOUT_SECONDS = 120

# Cutoff: only keep purchases from the last 7 days
CUTOFF_DAYS = 7
CUTOFF_DATE = datetime.utcnow() - timedelta(days=CUTOFF_DAYS)

# Fields for the detailed purchase history CSV
purchase_history_fields = [
    "rics_id", "email", "first_name", "last_name", "orders", "total_spent", "city", "state", "zip", "phone",
    "TicketDateTime", "TicketNumber", "Change", "TicketVoided", "ReceiptPrinted", "TicketSuspended", "ReceiptEmailed",
    "SaleDateTime", "TicketModifiedOn", "ModifiedBy", "CreatedOn",
    "TicketLineNumber", "Quantity", "AmountPaid", "Sku", "Summary", "Description", "SupplierCode", "SupplierName",
    "Color", "Column", "Row", "OnHand"
]

def parse_sale_datetime(val):
    """Try to parse RICS datetime strings safely."""
    if not val:
        return None
    try:
        return datetime.strptime(val, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(val, "%m/%d/%Y %H:%M:%S")
        except Exception:
            return None

def fetch_purchase_history_for_customer(cust_id, customer_info, max_purchase_pages=None, debug_mode=False):
    ph_skip = 0
    ph_take = 100
    all_rows = []
    page_count = 0
    api_calls = 0

    while True:
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
                    log_message(f"⚠️ Purchase history failed for customer {cust_id}: {ph_data.get('Message')}")
                    continue
                sale_headers = ph_data.get("SaleHeaders", [])
                log_message(f"✅ Used header for purchase history: {list(ph_headers.keys())[0]}")
                break
            except Exception as e:
                log_message(f"❌ Error fetching purchase history for customer {cust_id} with header {list(ph_headers.keys())[0]}: {e}")
                continue
        if not sale_headers:
            break

        for sale in sale_headers:
            sale_dt = parse_sale_datetime(sale.get("SaleDateTime") or sale.get("TicketDateTime"))
            if sale_dt and sale_dt < CUTOFF_DATE:
                # Skip old sales
                continue

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

        result_stats = ph_data.get("ResultStatistics", {})
        end_record = result_stats.get("EndRecord", 0)
        total_records = result_stats.get("TotalRecords", 0)
        page_count += 1
        if (max_purchase_pages is not None and page_count >= max_purchase_pages) or end_record >= total_records:
            break
        ph_skip += ph_take
        if debug_mode:
            break

    log_message(f"📦 Customer {cust_id} purchase history (filtered): {len(all_rows)} rows, {page_count} pages, {api_calls} API calls")
    return all_rows

# fetch_rics_data_with_purchase_history stays unchanged
# (it just calls fetch_purchase_history_for_customer and writes rows to CSV)
