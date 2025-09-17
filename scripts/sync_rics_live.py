import os
import sys
import csv
import json
import time
import logging
import requests
from datetime import datetime, timedelta

# === Setup logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Environment variables ===
RICS_API_TOKEN = os.getenv("RICS_API_TOKEN")
GDRIVE_FOLDER_ID_RICS = os.getenv("GDRIVE_FOLDER_ID_RICS")

if not RICS_API_TOKEN:
    logging.error("Missing RICS_API_TOKEN in environment")
    sys.exit(1)

# === File paths ===
timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
base_filename = f"rics_customer_purchase_history_{timestamp}.csv"
latest_filename = "rics_customer_purchase_history_latest.csv"
deduped_filename = "rics_customer_purchase_history_deduped.csv"

# === API Config ===
RICS_API_BASE = "https://api.ricssoftware.com/pos/GetPOSTransaction"
STORE_CODES = os.getenv("RICS_STORE_CODES", "").split(",")  # comma-separated store codes
LOOKBACK_DAYS = 7

# === Helper: write CSV ===
def write_csv(filename, rows, headers):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    logging.info(f"Wrote {len(rows)} rows → {filename}")

# === Fetch with pagination ===
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
            break  # last page
        else:
            page += 1
            time.sleep(0.25)  # be nice to the API

    logging.info(f"Store {store_code}: fetched {len(all_rows)} transactions total")
    return all_rows

# === Deduplicate (by transactionId) ===
def dedupe_rows(rows):
    seen = set()
    deduped = []
    for row in rows:
        tid = row.get("transactionId")
        if tid not in seen:
            deduped.append(row)
            seen.add(tid)
    return deduped

# === Main ===
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
        write_csv(base_filename.replace(".csv", "_EMPTY.csv"), [], headers)
        sys.exit(0)

    deduped = dedupe_rows(all_transactions)

    headers = list(deduped[0].keys())

    # Write all versions
    write_csv(base_filename, all_transactions, headers)
    write_csv(latest_filename, all_transactions, headers)
    write_csv(deduped_filename, deduped, headers)

    logging.info(f"Final counts → raw: {len(all_transactions)}, deduped: {len(deduped)}")
    logging.info("=== Finished sync_rics_live.py ===")

if __name__ == "__main__":
    main()
