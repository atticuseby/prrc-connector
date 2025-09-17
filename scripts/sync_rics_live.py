import os
import sys
import csv
import time
import logging
import requests
from datetime import datetime, timedelta

from upload_to_gdrive import upload_to_drive

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

RICS_API_TOKEN = os.getenv("RICS_API_TOKEN")
LOOKBACK_DAYS = 7

if not RICS_API_TOKEN:
    logging.error("Missing RICS_API_TOKEN in environment")
    sys.exit(1)

timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
base_filename = f"rics_customer_purchase_history_{timestamp}.csv"
latest_filename = "rics_customer_purchase_history_latest.csv"
deduped_filename = "rics_customer_purchase_history_deduped.csv"

RICS_API_BASE = "https://api.ricssoftware.com/pos/GetPOSTransaction"
STORE_CODES = os.getenv("RICS_STORE_CODES", "").split(",")

def write_csv(filename, rows, headers):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logging.info(f"Wrote {len(rows)} rows → {filename}")

def fetch_transactions(store_code, start_date, end_date):
    page = 1
    per_page = 100
    all_rows = []

    while True:
        params = {
            "storeCode": store_code,
            "startDate": start_date,
            "endDate": end_date,
            "page": page,
            "pageSize": per_page,
            "includeVoided": "false"
        }

        logging.info(f"Fetching store {store_code}, page {page}, params={params}")

        try:
            resp = requests.get(
                RICS_API_BASE,
                headers={"Authorization": f"Bearer {RICS_API_TOKEN}"},
                params=params,
                timeout=30
            )
            resp.raise_for_status()
        except Exception as e:
            logging.error(f"RICS API error: {e}")
            break

        data = resp.json()
        transactions = data.get("transactions", [])

        if not transactions:
            logging.info("No more transactions found")
            break

        all_rows.extend(transactions)

        if len(transactions) < per_page:
            break
        else:
            page += 1
            time.sleep(0.25)

    logging.info(f"Store {store_code}: fetched {len(all_rows)} transactions total")
    return all_rows

def dedupe_rows(rows):
    seen = set()
    deduped = []
    for row in rows:
        tid = row.get("transactionId")
        if tid not in seen:
            deduped.append(row)
            seen.add(tid)
    return deduped

def main():
    start_date = (datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    logging.info(f"=== Starting sync_rics_live.py ===")
    logging.info(f"Date window: {start_date} → {end_date}")
    logging.info(f"Store codes: {STORE_CODES}")

    all_transactions = []

    for store in STORE_CODES:
        store = store.strip()
        if not store:
            continue
        rows = fetch_transactions(store, start_date, end_date)
        all_transactions.extend(rows)

    if not all_transactions:
        logging.warning("No transactions found. Writing EMPTY.csv for clarity.")
        headers = ["transactionId", "customerId", "storeCode", "amount", "date"]
        empty_file = base_filename.replace(".csv", "_EMPTY.csv")
        write_csv(empty_file, [], headers)
        upload_to_drive(empty_file, os.path.basename(empty_file))
        sys.exit(0)

    deduped = dedupe_rows(all_transactions)
    headers = list(deduped[0].keys())

    write_csv(base_filename, all_transactions, headers)
    write_csv(latest_filename, all_transactions, headers)
    write_csv(deduped_filename, deduped, headers)

    upload_to_drive(base_filename)
    upload_to_drive(latest_filename)
    upload_to_drive(deduped_filename)

    logging.info(f"Final counts → raw: {len(all_transactions)}, deduped: {len(deduped)}")
    logging.info("=== Finished sync_rics_live.py ===")

if __name__ == "__main__":
    main()
