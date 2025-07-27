import csv
import sys
import os
from datetime import datetime, timedelta

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else 'optimizely_connector/output/rics_customer_purchase_history_latest.csv'
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else 'optimizely_connector/output/rics_cleaned_last24h.csv'

seen = set()
now = datetime.now()
seven_days_ago = now - timedelta(days=7)

def parse_datetime(raw):
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip(), fmt)
        except:
            continue
    return None

with open(INPUT_PATH, newline='', encoding='utf-8') as infile, \
     open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    kept = 0
    skipped = 0

    for row in reader:
        # Skip if no email or phone
        if not row.get("email") and not row.get("phone"):
            skipped += 1
            continue

        # Skip if voided or suspended
        if row.get("TicketVoided") == 'TRUE' or row.get("TicketSuspended") == 'TRUE':
            skipped += 1
            continue

        # Validate TicketDateTime
        ticket_time_raw = row.get("TicketDateTime", "")
        ticket_time = parse_datetime(ticket_time_raw)
        if not ticket_time or ticket_time < seven_days_ago or ticket_time > now + timedelta(minutes=1):
            skipped += 1
            continue

        # Validate AmountPaid
        try:
            amount = float(row.get("AmountPaid", 0))
            if amount <= 0:
                skipped += 1
                continue
        except:
            skipped += 1
            continue

        # Dedup by rics_id or email
        key = row.get("rics_id") or row.get("email")
        if key in seen:
            skipped += 1
            continue
        seen.add(key)

        writer.writerow(row)
        kept += 1

print(f"✅ Done. {kept} valid rows written to {OUTPUT_PATH}. {skipped} skipped.")
