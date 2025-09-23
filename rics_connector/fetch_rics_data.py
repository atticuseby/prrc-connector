import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import csv
import time
import requests
from datetime import datetime, timedelta
from scripts.helpers import log_message
import concurrent.futures
import argparse

# --- CONFIGURATION ---
TEST_MODE = False
MAX_PURCHASE_PAGES = None
MAX_WORKERS = 1  # Reduced to 1 to avoid rate limiting
DEBUG_MODE = False

ABSOLUTE_TIMEOUT_SECONDS = 120
CUTOFF_DATE = datetime.utcnow() - timedelta(days=7)  # 7 days from NOW to capture recent data
log_message(f"🔍 DEBUG: Current UTC time: {datetime.utcnow()}")
log_message(f"🔍 DEBUG: Cutoff date: {CUTOFF_DATE}")

purchase_history_fields = [
    "TicketDateTime", "TicketNumber", "SaleDateTime", "StoreCode", "TerminalId", "Cashier",
    "AccountNumber", "CustomerId",
    "Sku", "Description", "Quantity", "AmountPaid", "Discount", "Department", "SupplierName"
]

DEDUP_LOG_PATH = os.path.join("logs", "sent_ticket_ids.csv")


def parse_dt(dt_str):
    if not dt_str:
        return None
    
    # Try common date formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S", 
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%Y %H:%M:%S", 
        "%m/%d/%Y %H:%M",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ"
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    
    # If all formats fail, log the first few attempts for debugging
    if not hasattr(parse_dt, '_debug_count'):
        parse_dt._debug_count = 0
    
    if parse_dt._debug_count < 3:
        log_message(f"🔍 DEBUG: Could not parse date string: '{dt_str}' (first {parse_dt._debug_count + 1} attempts)")
        parse_dt._debug_count += 1
    
    return None


def load_sent_ticket_ids():
    """Load sent ticket IDs from local file if present."""
    try:
        with open(DEDUP_LOG_PATH, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()


def save_sent_ticket_ids(ticket_ids):
    """Save updated sent ticket IDs locally (workflow uploads logs to Drive)."""
    os.makedirs(os.path.dirname(DEDUP_LOG_PATH), exist_ok=True)
    with open(DEDUP_LOG_PATH, "w") as f:
        for tid in sorted(ticket_ids):
            f.write(f"{tid}\n")


def fetch_pos_transactions_for_store(store_code=None,
                                     max_purchase_pages=None,
                                     debug_mode=False,
                                     already_sent=None):
    """Fetch purchase history from POS/GetPOSTransaction for a given store."""
    start_time = datetime.utcnow()
    all_rows = []
    seen_keys = set()
    page_count, api_calls, skip, take = 0, 0, 0, 100

    start_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")  # 7 days to find recent data
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    log_message(f"🔍 DEBUG: API date range - Start: {start_date}, End: {end_date}")
    log_message(f"🔍 DEBUG: Current year: {datetime.utcnow().year}")

    while True:
        # Safety check: prevent infinite loops (check at start of each iteration)
        if page_count > 50:  # Max 50 pages per store
            log_message(f"⏰ Store {store_code}: Hit max pages limit ({page_count})")
            break
            
        # Timeout check: prevent individual stores from running too long
        if (datetime.utcnow() - start_time).total_seconds() > 300:  # 5 minutes per store
            log_message(f"⏰ Store {store_code}: Hit 5-minute timeout")
            break
            
        payload = {
            "Take": take,
            "Skip": skip,
            "TicketDateStart": start_date,
            "TicketDateEnd": end_date,
            "BatchStartDate": start_date,   # Added
            "BatchEndDate": end_date,       # Added
            "StoreCode": store_code
        }

        try:
            log_message(f"📤 Fetching POS transactions for Store {store_code}, "
                        f"page {page_count+1}")
            
            # Check if token is available
            token = os.getenv("RICS_API_TOKEN")
            if not token:
                log_message(f"❌ RICS_API_TOKEN not found for Store {store_code}")
                break
                
            resp = requests.post(
                "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction",
                headers={"Token": token},
                json=payload,
                timeout=30  # Reduced from 120 to 30 seconds per API call
            )
            api_calls += 1
            
            log_message(f"📊 Store {store_code} API response: {resp.status_code}")
            
            if resp.status_code == 401:
                log_message(f"❌ 401 Unauthorized for Store {store_code} - token may be invalid or expired")
                break
            elif resp.status_code == 429:
                log_message(f"⚠️ Rate limited for Store {store_code} - waiting 30 seconds before retry")
                import time
                time.sleep(30)  # Wait 30 seconds for rate limit to reset
                continue  # Retry the same request
            elif resp.status_code != 200:
                log_message(f"❌ API error {resp.status_code} for Store {store_code}: {resp.text[:200]}")
                break
                
            resp.raise_for_status()
            data = resp.json()

            sales = data.get("Sales", [])
            log_message(f"📊 Store {store_code} returned {len(sales)} sales")
            
            # Minimal debugging - only log if no sales found
            if not sales and page_count < 3:
                log_message(f"🔍 Debug - Store {store_code} page {page_count+1}: No sales in response")
            
            # Add delay to avoid rate limiting
            import time
            time.sleep(2)  # 2 second delay between API calls
            
            if not sales:
                log_message(f"⚠️ No more Sales returned for Store {store_code}.")
                break
            
            # Check if we're getting duplicate data (same sales count on multiple pages)
            if page_count > 2 and len(sales) > 0:
                # If we've seen the same sales count 3 times in a row, likely duplicate data
                if not hasattr(fetch_pos_transactions_for_store, '_last_sales_count'):
                    fetch_pos_transactions_for_store._last_sales_count = {}
                if not hasattr(fetch_pos_transactions_for_store, '_sales_count_repeats'):
                    fetch_pos_transactions_for_store._sales_count_repeats = {}
                
                store_key = f"store_{store_code}"
                current_count = len(sales)
                
                if store_key in fetch_pos_transactions_for_store._last_sales_count:
                    if fetch_pos_transactions_for_store._last_sales_count[store_key] == current_count:
                        fetch_pos_transactions_for_store._sales_count_repeats[store_key] = fetch_pos_transactions_for_store._sales_count_repeats.get(store_key, 0) + 1
                        if fetch_pos_transactions_for_store._sales_count_repeats[store_key] >= 3:
                            log_message(f"⚠️ Store {store_code}: Detected duplicate data ({current_count} sales repeated 3+ times), stopping pagination")
                            break
                    else:
                        fetch_pos_transactions_for_store._sales_count_repeats[store_key] = 0
                
                fetch_pos_transactions_for_store._last_sales_count[store_key] = current_count

            for sale in sales:
                # The actual sales are in the SaleHeaders field
                sale_headers = sale.get("SaleHeaders", [])
                
                if not sale_headers:
                    continue  # Skip if no sale headers
                
                # Debug: Log the actual sale data structure for first few sales
                if len(all_rows) < 3:  # Only log first 3 sales for debugging
                    log_message(f"🔍 DEBUG: Sale data keys: {list(sale.keys())}")
                    log_message(f"🔍 DEBUG: SaleHeaders count: {len(sale_headers)}")
                    if sale_headers:
                        log_message(f"🔍 DEBUG: First SaleHeader keys: {list(sale_headers[0].keys())}")
                
                # Process each sale header (individual transaction)
                for sale_header in sale_headers:
                    sale_dt = parse_dt(sale_header.get("TicketDateTime") or sale_header.get("SaleDateTime"))
                    
                    if len(all_rows) < 3:  # Only log first 3 sales for debugging
                        log_message(f"🔍 Sale {sale_header.get('TicketNumber')}: date={sale_dt}, cutoff={CUTOFF_DATE}")
                    
                    if not sale_dt or sale_dt < CUTOFF_DATE:
                        continue  # Skip old sales

                    sale_info = {
                        "TicketDateTime": sale_header.get("TicketDateTime"),
                        "TicketNumber": sale_header.get("TicketNumber"),
                        "SaleDateTime": sale_header.get("SaleDateTime"),
                        "StoreCode": sale.get("StoreCode"),
                        "TerminalId": sale_header.get("TerminalId"),
                        "Cashier": sale_header.get("CashierName"),
                        "AccountNumber": sale_header.get("AccountNumber"),
                        "CustomerId": sale_header.get("CustomerId"),
                    }

                    for item in sale_header.get("SaleLines", []):
                        key = f"{sale_info['TicketNumber']}_{item.get('Sku')}"
                        if already_sent and sale_info['TicketNumber'] in already_sent:
                            continue
                        if key in seen_keys:
                            continue

                        seen_keys.add(key)
                        row = {
                            **sale_info,
                            "Sku": item.get("Sku"),
                            "Description": item.get("Description"),
                            "Quantity": item.get("Quantity"),
                            "AmountPaid": item.get("AmountPaid"),
                            "Discount": item.get("DiscountAmount"),
                            "Department": item.get("Department"),
                            "SupplierName": item.get("SupplierName"),
                        }
                        all_rows.append(row)

            page_count += 1
            
            # Safety check: prevent infinite loops (check BEFORE processing)
            if page_count > 50:  # Max 50 pages per store
                log_message(f"⏰ Store {store_code}: Hit max pages limit ({page_count})")
                break
                
            if max_purchase_pages and page_count >= max_purchase_pages:
                break
                
            skip += take
            if debug_mode:
                break

        except Exception as e:
            log_message(f"❌ Error fetching POS transactions for Store {store_code}: {e}")
            break

    log_message(f"📦 Store {store_code}: Collected {len(all_rows)} new rows "
                f"({page_count} pages, {api_calls} calls)")
    return all_rows


def fetch_rics_data_with_purchase_history(max_purchase_pages=None, debug_mode=False, return_summary=False):
    start_time = datetime.utcnow()
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_customer_purchase_history_{timestamp}.csv"
    output_dir = os.path.join("optimizely_connector", "output")
    output_path = os.path.join(output_dir, filename)
    os.makedirs(output_dir, exist_ok=True)

    already_sent = load_sent_ticket_ids()
    log_message(f"📂 Loaded {len(already_sent)} previously sent TicketNumbers")

    all_rows = []
    STORE_CODES = [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                fetch_pos_transactions_for_store,
                store_code=store_code,
                max_purchase_pages=max_purchase_pages,
                debug_mode=debug_mode,
                already_sent=already_sent
            ): store_code
            for store_code in STORE_CODES
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                # Check if we've been running too long (10 minutes max)
                if (datetime.utcnow() - start_time).total_seconds() > 600:
                    log_message(f"⏰ Hit 10-minute timeout, stopping processing")
                    break
                    
                rows = future.result()
                all_rows.extend(rows)
            except Exception as exc:
                log_message(f"❌ Error in thread for store {futures[future]}: {exc}")

    with open(output_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=purchase_history_fields)
        writer.writeheader()
        writer.writerows(all_rows)

    log_message(f"📝 Wrote {len(all_rows)} rows to {output_path}")

    new_ticket_ids = {row["TicketNumber"] for row in all_rows}
    skipped_count = len([tid for tid in already_sent if tid not in new_ticket_ids])

    if new_ticket_ids:
        updated_sent = already_sent.union(new_ticket_ids)
        save_sent_ticket_ids(updated_sent)
        summary = f"{len(new_ticket_ids)} new tickets, {skipped_count} skipped (already sent)"
        log_message(f"✅ Dedup log updated: {len(updated_sent)} total TicketNumbers tracked")
        log_message(f"📊 Summary: {summary}")
    else:
        summary = "0 new tickets (all skipped)"
        log_message("⚠️ No new TicketNumbers found in this run.")

    if return_summary:
        return output_path, summary
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch RICS POS data with purchase history.")
    parser.add_argument('--test', action='store_true', help='Enable test mode (limits stores and pages)')
    parser.add_argument('--max-purchase-pages', type=int, help='Override max purchase pages')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    args = parser.parse_args()

    if args.test:
        TEST_MODE = True
        MAX_PURCHASE_PAGES, MAX_WORKERS = 1, 2
    if args.max_purchase_pages:
        MAX_PURCHASE_PAGES = args.max_purchase_pages
    if args.debug:
        DEBUG_MODE = True

    fetch_rics_data_with_purchase_history(MAX_PURCHASE_PAGES, DEBUG_MODE)
