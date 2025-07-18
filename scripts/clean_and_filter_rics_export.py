import csv
import sys
import os
from datetime import datetime, timedelta

# Usage: python scripts/clean_and_filter_rics_export.py [input_csv] [output_csv]
INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else 'data/rics_customer_purchase_history_latest.csv'
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else 'data/rics_cleaned_last24h.csv'

# Columns to keep for both Meta and Optimizely
COLUMNS_TO_KEEP = [
    "rics_id", "email", "first_name", "last_name", "city", "state", "zip", "phone",
    "TicketDateTime", "TicketNumber", "Change", "TicketVoided", "ReceiptPrinted", "TicketSuspended", "ReceiptEmailed", "SaleDateTime", "TicketModifiedOn", "ModifiedBy", "CreatedOn",
    "TicketLineNumber", "Quantity", "AmountPaid", "Sku", "Summary", "Description", "SupplierCode", "SupplierName", "Color", "Column", "Row", "OnHand"
]

# Try to parse a datetime string

def parse_dt(dt_str):
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y %H:%M", "%Y-%m-%d"):  # Add more as needed
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    return None

def filter_last_24h(input_path, output_path):
    now = datetime.now()
    cutoff = now - timedelta(hours=24)
    kept_rows = []
    with open(input_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Use TicketDateTime or SaleDateTime for filtering
            dt_str = row.get("TicketDateTime") or row.get("SaleDateTime")
            dt = parse_dt(dt_str) if dt_str else None
            if dt and dt >= cutoff:
                kept_rows.append({k: row.get(k, "") for k in COLUMNS_TO_KEEP})
    if not kept_rows:
        print("⚠️ No rows found in the last 24 hours.")
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS_TO_KEEP)
        writer.writeheader()
        writer.writerows(kept_rows)
    print(f"✅ Wrote cleaned, filtered CSV: {output_path} ({len(kept_rows)} rows from last 24h)")

if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Input file not found: {INPUT_PATH}")
        sys.exit(1)
    filter_last_24h(INPUT_PATH, OUTPUT_PATH) 