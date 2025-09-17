import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import csv
import time
import requests
from datetime import datetime, timedelta
from scripts.helpers import log_message
import concurrent.futures
import argparse
import json

# --- CONFIGURATION ---
TEST_MODE = False
MAX_PURCHASE_PAGES = None
MAX_WORKERS = 3
DEBUG_MODE = False

ABSOLUTE_TIMEOUT_SECONDS = 120
CUTOFF_DATE = datetime.utcnow() - timedelta(days=7)

purchase_history_fields = [
    "TicketDateTime", "TicketNumber", "SaleDateTime", "StoreCode", "TerminalId", "Cashier",
    "AccountNumber", "CustomerId",
    "Sku", "Description", "Quantity", "AmountPaid", "Discount", "Department", "SupplierName"
]

DEDUP_LOG_PATH = os.path.join("logs", "sent_ticket_ids.csv")
RICS_ENDPOINT = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"  # ✅ correct endpoint


def parse_dt(dt_str):
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ",
                "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"):
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    return None


def load_sent_ticket_ids():
    try:
        with open(DEDUP_LOG_PATH, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()


def save_sent_ticket_ids(ticket_ids):
    os.makedirs(os.path.dirname(DEDUP_LOG_PATH), exist_ok=True)
    with open(DEDUP_LOG_PATH, "w") as f:
        for tid in sorted(ticket_ids):
            f.write(f"{tid}\n")


def fetch_pos_transactions_for_store(store_code=None,
                                     max_purchase_pages=None,
                                     debug_mode=False,
                                     already_sent=None):
    all_rows = []
    seen_keys = set()
    page_count, api_calls, skip, take = 0, 0, 0, 100

    start_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        payload = {
            "Take": take,
            "Skip": skip,
            "TicketDateStart": start_date,
            "TicketDateEnd": end_date,
            "SaleDateStart": start_date,   # ✅ add both sets
            "SaleDateEnd": end_date,
            "StoreCode": store_code
        }

        try:
            log_message(f"📤 Fetching POS transactions for Store {store_code}, page {page_count+1}")
            resp = requests.post(
                RICS_ENDPOINT,
                headers={"Token": os.getenv("RICS_API_TOKEN")},
                json=payload,
                timeout=ABSOLUTE_TIMEOUT_SECONDS
            )
            api_calls += 1
            resp.raise_for_status()
            data = resp.json()

            # --- Debugging output ---
            try:
                log_message(f"🔍 Raw response excerpt (store {store_code}, page {page_count+1}): {resp.text[:500]}")
            except Exception:
                log_message("⚠️ Could not log raw response text.")

            sales = data.get("Sales", [])
            if not sales:
                log_message(f"⚠️ No Sales returned for Store {store_code}, page {page_count+1}.")
                break

            for sale in sales:
                sale_dt = parse_dt(sale.get("TicketDateTime") or sale.get("SaleDateTime"))
                if not sale_dt or sale_dt < CUTOFF_DATE:
                    continue

                sale_info = {
                    "TicketDateTime": sale.get("TicketDateTime"),
                    "TicketNumber": sale.get("TicketNumber"),
                    "SaleDateTime": sale.get("SaleDateTime"),
                    "StoreCode": sale.get("StoreCode"),
                    "TerminalId": sale.get("TerminalId"),
                    "Cashier": sale.get("CashierName"),
                    "AccountNumber": sale.get("AccountNumber"),
                    "CustomerId": sale.get("CustomerId"),
                }

                for item in sale.get("SaleLines", []):
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
            if max_purchase_pages and page_count >= max_purchase_pages:
                break
            skip += take
            if debug_mode:
                break

        except Exception as e:
            log_message(f"❌ Error fetching POS transactions for Store {store_code}: {e}")
            break

    log_message(f"📦 Store {store_code}: Collected {len(all_rows)} rows "
                f"({page_count} pages, {api_calls} calls)")
    return all_rows


def fetch_rics_data_with_purchase_history(max_purchase_pages=None, debug_mode=False, return_summary=False):
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_customer_purchase_history_{timestamp}.csv"
    output_dir = os.path.join("optimizely_connector", "output")
    output_path = os.path.join(output_dir, filename)
    os.makedirs(output_dir, exist_ok=True)

    already_sent = load_sent_ticket_ids()
    log_message(f"📂 Loaded {len(already_sent)} previously sent TicketNumbers")

    store_codes_env = os.getenv("RICS_STORE_CODES", "").strip()
    if store_codes_env:
        STORE_CODES = [int(code.strip()) for code in store_codes_env.split(",") if code.strip().isdigit()]
        log_message(f"✅ Loaded store codes from env var: {STORE_CODES}")
    else:
        STORE_CODES = [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99]
        log_message(f"⚠️ Using fallback store codes: {STORE_CODES}")

    if not STORE_CODES:
        log_message("❌ ERROR: No store codes provided. Aborting run.")
        return None

    all_rows = []
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
                rows = future.result()
                all_rows.extend(rows)
            except Exception as exc:
                log_message(f"❌ Error in thread for store {futures[future]}: {exc}")

    with open(output_path, "w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=purchase_history_fields)
        writer.writeheader()
        writer.writerows(all_rows)

    if not all_rows:
        empty_file = output_path.replace(".csv", "_EMPTY.csv")
        with open(empty_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=purchase_history_fields)
            writer.writeheader()
        log_message(f"⚠️ No rows found. Wrote EMPTY file → {empty_file}")
        return empty_file if not return_summary else (empty_file, "0 rows")

    log_message(f"📝 Wrote {len(all_rows)} rows to {output_path}")

    new_ticket_ids = {row["TicketNumber"] for row in all_rows}
    if new_ticket_ids:
        updated_sent = already_sent.union(new_ticket_ids)
        save_sent_ticket_ids(updated_sent)
        log_message(f"✅ Dedup log updated: {len(updated_sent)} total TicketNumbers tracked")

    summary = f"{len(all_rows)} rows written"
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
