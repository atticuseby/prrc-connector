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

# Configurable lookback days via environment variable
# Default to 1 day for daily syncs, but can be set to 45 for initial catch-up sync
RICS_LOOKBACK_DAYS = int(os.getenv("RICS_LOOKBACK_DAYS", "1"))
CUTOFF_DATE = datetime.utcnow() - timedelta(days=RICS_LOOKBACK_DAYS)
log_message(f"🔍 DEBUG: Current UTC time: {datetime.utcnow()}")
log_message(f"🔍 DEBUG: Lookback days: {RICS_LOOKBACK_DAYS}")
log_message(f"🔍 DEBUG: Cutoff date: {CUTOFF_DATE}")

purchase_history_fields = [
    "TicketDateTime", "TicketNumber", "SaleDateTime", "StoreCode", "TerminalId", "Cashier",
    "AccountNumber", "CustomerId", "CustomerName", "CustomerEmail", "CustomerPhone",
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
            return set(str(line.strip()) for line in f if line.strip())
    except FileNotFoundError:
        return set()


def save_sent_ticket_ids(ticket_ids):
    """Save updated sent ticket IDs locally (workflow uploads logs to Drive)."""
    os.makedirs(os.path.dirname(DEDUP_LOG_PATH), exist_ok=True)
    with open(DEDUP_LOG_PATH, "w") as f:
        # Convert all ticket IDs to strings before sorting to avoid type comparison errors
        for tid in sorted(str(tid) for tid in ticket_ids):
            f.write(f"{tid}\n")


def fetch_pos_transactions_for_store(store_code=None,
                                     max_purchase_pages=None,
                                     debug_mode=False,
                                     already_sent=None,
                                     lookback_days=None):
    """
    Fetch purchase history from POS/GetPOSTransaction for a given store.
    
    Args:
        store_code: Store code to fetch data for
        max_purchase_pages: Maximum pages to fetch (None = unlimited)
        debug_mode: Enable debug logging
        already_sent: Set of ticket IDs already processed (for deduplication)
        lookback_days: Number of days to look back (defaults to RICS_LOOKBACK_DAYS env var or 1)
    """
    start_time = datetime.utcnow()
    all_rows = []
    seen_keys = set()
    page_count, api_calls, skip, take = 0, 0, 0, 100

    # Use provided lookback_days or fall back to global config
    if lookback_days is None:
        lookback_days = RICS_LOOKBACK_DAYS
    
    # Use ISO 8601 format with time (YYYY-MM-DDTHH:MM:SSZ) for both Batch and Ticket date fields
    # Per API docs: use both BatchStartDate/BatchEndDate AND TicketDateStart/TicketDateEnd
    # Calculate date range: lookback_days ago to now
    start_datetime = datetime.utcnow() - timedelta(days=lookback_days)
    end_datetime = datetime.utcnow()
    
    # Format as ISO 8601 with time (start of day for start, end of day for end)
    start_date = start_datetime.strftime("%Y-%m-%dT00:00:00Z")
    end_date = end_datetime.strftime("%Y-%m-%dT23:59:59Z")
    
    # Also create date-only versions for Batch dates (try both formats)
    start_date_only = start_datetime.strftime("%Y-%m-%d")
    end_date_only = end_datetime.strftime("%Y-%m-%d")
    
    log_message(f"🔍 DEBUG: Date range calculation:")
    log_message(f"   lookback_days: {lookback_days}")
    log_message(f"   start_datetime: {start_datetime} (UTC)")
    log_message(f"   end_datetime: {end_datetime} (UTC)")
    log_message(f"   API will query: {start_date} to {end_date}")
    
    log_message(f"🔍 Store {store_code}: API date range - Start: {start_date} / {start_date_only}, End: {end_date} / {end_date_only} ({lookback_days} days lookback)")
    log_message(f"🔍 DEBUG: Using both BatchStartDate/BatchEndDate (date-only) AND TicketDateStart/TicketDateEnd (ISO 8601)")
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
            
        # Use both BatchStartDate/BatchEndDate AND TicketDateStart/TicketDateEnd
        # Try date-only format for Batch dates, ISO 8601 with time for Ticket dates
        payload = {
            "Take": take,
            "Skip": skip,
            "BatchStartDate": start_date_only,  # Date-only format
            "BatchEndDate": end_date_only,        # Date-only format
            "TicketDateStart": start_date,        # ISO 8601 with time
            "TicketDateEnd": end_date,            # ISO 8601 with time
            "StoreCode": str(store_code)
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
            
            # Check what date range is actually in this page of results
            if sales and page_count == 0:
                page_dates = []
                for sale in sales:
                    sale_headers = sale.get("SaleHeaders", [])
                    for header in sale_headers:
                        dt_str = header.get("TicketDateTime") or header.get("SaleDateTime")
                        if dt_str:
                            dt = parse_dt(dt_str)
                            if dt:
                                page_dates.append(dt)
                
                if page_dates:
                    page_oldest = min(page_dates)
                    page_newest = max(page_dates)
                    log_message(f"📅 Store {store_code} page 1 date range: {page_oldest} to {page_newest}")
                    log_message(f"📅 Store {store_code} requested range: {start_date_only} to {end_date_only} (Batch) / {start_date} to {end_date} (Ticket)")
                    if page_newest < (datetime.utcnow() - timedelta(days=7)):
                        days_behind = (datetime.utcnow() - page_newest).days
                        log_message(f"⚠️  WARNING: Store {store_code} newest data is {days_behind} days old!")
            
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
                    
                    # REMOVED: Post-fetch date filtering
                    # The API query date range (BatchStartDate/BatchEndDate and TicketDateStart/TicketDateEnd)
                    # should be the ONLY filter. If the API returns it, we use it.
                    # This prevents double-filtering that could exclude valid data.
                    
                    if not sale_dt:
                        if len(all_rows) < 10:  # Log first few missing dates
                            log_message(f"⚠️ Skipping sale {sale_header.get('TicketNumber')} - could not parse date")
                        continue
                    
                    # Log first few dates for debugging
                    if len(all_rows) < 3:
                        log_message(f"🔍 Sale {sale_header.get('TicketNumber')}: date={sale_dt}")
                        log_message(f"🔍 Raw TicketDateTime: {sale_header.get('TicketDateTime')}")
                        log_message(f"🔍 Raw SaleDateTime: {sale_header.get('SaleDateTime')}")

                    # Get customer info if available
                    customer_info = sale_header.get("Customer", {})
                    
                    # Debug: Log customer data for first few sales
                    if len(all_rows) < 3:
                        log_message(f"🔍 DEBUG: Customer data: {customer_info}")
                    
                    sale_info = {
                        "TicketDateTime": sale_header.get("TicketDateTime"),
                        "TicketNumber": str(sale_header.get("TicketNumber", "")),
                        "SaleDateTime": sale_header.get("SaleDateTime"),
                        "StoreCode": sale.get("StoreCode"),
                        "TerminalId": sale_header.get("TerminalId"),
                        "Cashier": sale_header.get("CashierName"),
                        "AccountNumber": customer_info.get("AccountNumber"),
                        "CustomerId": customer_info.get("CustomerId"),
                        "CustomerName": customer_info.get("FirstName", "") + " " + customer_info.get("LastName", ""),
                        "CustomerEmail": customer_info.get("Email"),
                        "CustomerPhone": customer_info.get("Phone"),
                    }

                    # Check if there are SaleDetails (items) for this sale
                    sale_details = sale_header.get("SaleDetails", [])
                    
                    # Debug: Log SaleDetails info for first few sales
                    if len(all_rows) < 5:
                        log_message(f"🔍 DEBUG: Sale {sale_header.get('TicketNumber')} has {len(sale_details)} SaleDetails")
                        if sale_details:
                            log_message(f"🔍 DEBUG: First SaleDetail: {sale_details[0]}")
                    
                    if sale_details:
                        # Process each item in the sale
                        for item in sale_details:
                            key = f"{sale_info['TicketNumber']}_{item.get('Sku')}"
                            if already_sent and sale_info['TicketNumber'] in already_sent:
                                continue
                            if key in seen_keys:
                                continue

                            seen_keys.add(key)
                            row = {
                                **sale_info,
                                "Sku": item.get("Sku"),
                                "Description": item.get("Summary") or item.get("TransactionSaleDescription"),
                                "Quantity": item.get("Quantity"),
                                "AmountPaid": item.get("AmountPaid"),
                                "Discount": item.get("PerkAmount", 0),
                                "Department": item.get("ProductItem", {}).get("Classes", [{}])[0].get("TagTree", ""),
                                "SupplierName": item.get("ProductItem", {}).get("Supplier"),
                            }
                            all_rows.append(row)
                    else:
                        # No SaleLines - add the sale header as a single row
                        key = f"{sale_info['TicketNumber']}_no_items"
                        if already_sent and sale_info['TicketNumber'] in already_sent:
                            continue
                        if key in seen_keys:
                            continue

                        seen_keys.add(key)
                        row = {
                            **sale_info,
                            "Sku": "",
                            "Description": "Sale (no items)",
                            "Quantity": 1,
                            "AmountPaid": sale_header.get("TotalAmount", 0),
                            "Discount": 0,
                            "Department": "",
                            "SupplierName": "",
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

    # Find the most recent and oldest dates in the collected rows
    if all_rows:
        dates = []
        for row in all_rows:
            dt = parse_dt(row.get("TicketDateTime") or row.get("SaleDateTime"))
            if dt:
                dates.append(dt)
        
        if dates:
            oldest_date = min(dates)
            newest_date = max(dates)
            log_message(f"📦 Store {store_code}: Collected {len(all_rows)} new rows "
                       f"({page_count} pages, {api_calls} calls)")
            log_message(f"📅 Store {store_code}: Date range in data - Oldest: {oldest_date}, Newest: {newest_date}")
            log_message(f"📅 Store {store_code}: Query range was - Start: {start_date_only} to {end_date_only} (Batch) / {start_date} to {end_date} (Ticket)")
            
            # Calculate how many days old the newest data is
            days_old = (datetime.utcnow() - newest_date).days
            if days_old > 7:
                log_message(f"⚠️  WARNING: Store {store_code} newest data is {days_old} days old! "
                           f"API may have a delay in data availability.")
        else:
            log_message(f"📦 Store {store_code}: Collected {len(all_rows)} new rows "
                       f"({page_count} pages, {api_calls} calls)")
            log_message(f"⚠️  WARNING: Store {store_code} rows have no parseable dates!")
    else:
        log_message(f"⚠️ WARNING: Store {store_code} returned no rows!")
        log_message(f"📅 Store {store_code}: Query range was - Start: {start_date}, End: {end_date}")
    
    return all_rows


def fetch_rics_data_with_purchase_history(max_purchase_pages=None, debug_mode=False, return_summary=False, no_dedup=False, lookback_days=None):
    """
    Fetch RICS purchase data from all stores.
    
    Args:
        max_purchase_pages: Maximum pages per store (None = unlimited)
        debug_mode: Enable debug logging
        return_summary: Return summary string along with file path
        no_dedup: Skip deduplication tracking
        lookback_days: Number of days to look back (defaults to RICS_LOOKBACK_DAYS env var or 1)
    """
    start_time = datetime.utcnow()
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_customer_purchase_history_{timestamp}.csv"
    output_dir = os.path.join("optimizely_connector", "output")
    output_path = os.path.join(output_dir, filename)
    os.makedirs(output_dir, exist_ok=True)

    # Use provided lookback_days or fall back to global config
    if lookback_days is None:
        lookback_days = RICS_LOOKBACK_DAYS
    
    log_message(f"📅 Fetching RICS data with {lookback_days} day(s) lookback")
    log_message(f"📅 Cutoff date: {CUTOFF_DATE}")
    log_message(f"📅 Query date range: {(datetime.utcnow() - timedelta(days=lookback_days)).strftime('%Y-%m-%d')} to {datetime.utcnow().strftime('%Y-%m-%d')}")
    log_message(f"📅 Current UTC time: {datetime.utcnow()}")
    log_message(f"📅 Using date-only format (YYYY-MM-DD) for BatchStartDate/BatchEndDate per RICS support")

    if no_dedup:
        log_message("🔧 No-dedup mode: Skipping deduplication")
        already_sent = set()
    else:
        already_sent = load_sent_ticket_ids()
        log_message(f"📂 Loaded {len(already_sent)} previously sent TicketNumbers")

    all_rows = []
    STORE_CODES = [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99]
    
    log_message(f"🏪 Processing {len(STORE_CODES)} stores: {STORE_CODES}")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                fetch_pos_transactions_for_store,
                store_code=store_code,
                max_purchase_pages=max_purchase_pages,
                debug_mode=debug_mode,
                already_sent=already_sent,
                lookback_days=lookback_days
            ): store_code
            for store_code in STORE_CODES
        }
        store_results = {}
        for future in concurrent.futures.as_completed(futures):
            try:
                # Check if we've been running too long (10 minutes max)
                if (datetime.utcnow() - start_time).total_seconds() > 600:
                    log_message(f"⏰ Hit 10-minute timeout, stopping processing")
                    break
                    
                store_code = futures[future]
                rows = future.result()
                all_rows.extend(rows)
                store_results[store_code] = len(rows)
                log_message(f"✅ Store {store_code}: Fetched {len(rows)} rows")
            except Exception as exc:
                store_code = futures[future]
                log_message(f"❌ Error in thread for store {store_code}: {exc}")
                store_results[store_code] = 0
        
        # Log summary of all stores
        log_message(f"\n📊 Store Summary ({lookback_days} day lookback):")
        for store_code in STORE_CODES:
            row_count = store_results.get(store_code, 0)
            status = "✅" if row_count > 0 else "⚠️"
            log_message(f"   {status} Store {store_code}: {row_count} rows")
        
        # Find overall date range across all stores
        if all_rows:
            dates = []
            for row in all_rows:
                dt = parse_dt(row.get("TicketDateTime") or row.get("SaleDateTime"))
                if dt:
                    dates.append(dt)
            
            if dates:
                oldest_date = min(dates)
                newest_date = max(dates)
                days_old = (datetime.utcnow() - newest_date).days
                log_message(f"\n📅 Overall Data Date Range:")
                log_message(f"   Oldest sale: {oldest_date}")
                log_message(f"   Newest sale: {newest_date}")
                log_message(f"   Newest data is {days_old} days old")
                if days_old > 7:
                    log_message(f"   ⚠️  WARNING: RICS API appears to have a {days_old}-day delay in data availability!")
                    log_message(f"   This is likely an API limitation, not a code issue.")

    # Debug: Log sample data before writing CSV
    if all_rows:
        log_message(f"🔍 DEBUG: Sample row data: {all_rows[0]}")
        log_message(f"🔍 DEBUG: Total rows to write: {len(all_rows)}")
    else:
        log_message(f"⚠️ WARNING: No rows to write to CSV!")

    with open(output_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=purchase_history_fields)
        writer.writeheader()
        writer.writerows(all_rows)

    log_message(f"📝 Wrote {len(all_rows)} rows to {output_path}")
    
    # Debug: Check if file was actually created and has content
    if os.path.exists(output_path):
        file_size = os.path.getsize(output_path)
        log_message(f"🔍 DEBUG: CSV file size: {file_size} bytes")
        if file_size > 0:
            with open(output_path, 'r') as f:
                lines = f.readlines()
                log_message(f"🔍 DEBUG: CSV has {len(lines)} lines")
                if len(lines) > 1:  # More than just header
                    log_message(f"🔍 DEBUG: First data line: {lines[1].strip()}")
    else:
        log_message(f"❌ ERROR: CSV file was not created!")

    if no_dedup:
        log_message("🔧 No-dedup mode: Skipping deduplication tracking")
        summary = f"{len(all_rows)} total rows (no dedup)"
    else:
        new_ticket_ids = {str(row["TicketNumber"]) for row in all_rows if row["TicketNumber"]}
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


# Alias for backward compatibility
fetch_rics_data = fetch_rics_data_with_purchase_history

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
