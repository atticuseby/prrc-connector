import csv
import sys
import os
from datetime import datetime, timedelta

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else 'optimizely_connector/output/rics_customer_purchase_history_latest.csv'
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else 'optimizely_connector/output/rics_cleaned_last24h.csv'

COLUMNS_TO_KEEP = [
    "rics_id", "email", "first_name", "last_name", "city", "state", "zip", "phone",
    "TicketDateTime", "TicketNumber", "Change", "TicketVoided", "ReceiptPrinted", "TicketSuspended", "ReceiptEmailed", "SaleDateTime", "TicketModifiedOn", "ModifiedBy", "CreatedOn",
    "TicketLineNumber", "Quantity", "AmountPaid", "Sku", "Summary", "Description", "SupplierCode", "SupplierName", "Color", "Column", "Row", "OnHand",
    "event_name", "event_time", "value", "currency"
]

def parse_dt(dt_str):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    return None

def to_unix(dt):
    return int(dt.timestamp())

def filter_last_24h(input_path, output_path):
    now = datetime.now()
    cutoff = now - timedelta(hours=24)
    kept_rows = []
    with open(input_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt_str = row.get("TicketDateTime") or row.get("SaleDateTime")
            dt = parse_dt(dt_str) if dt_str else None
            if dt and dt >= cutoff:
                new_row = {k: row.get(k, "") for k in COLUMNS_TO_KEEP if k in row}
                new_row["event_name"] = "Purchase"
                new_row["event_time"] = to_unix(dt)
                new_row["value"] = row.get("AmountPaid", "")
                new_row["currency"] = "USD"
                kept_rows.append(new_row)
    if not kept_rows:
        print("⚠️ No rows found in the last 24 hours.")
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS_TO_KEEP)
        writer.writeheader()
        writer.writerows(kept_rows)
    print(f"✅ Wrote Meta-ready CSV: {output_path} ({len(kept_rows)} rows from last 24h)")

if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Input file not found: {INPUT_PATH}")
        sys.exit(1)
    filter_last_24h(INPUT_PATH, OUTPUT_PATH)
