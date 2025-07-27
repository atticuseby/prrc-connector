import csv
import sys
import os
import hashlib

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else 'optimizely_connector/output/rics_customer_purchase_history_latest.csv'
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else 'meta_connector/processed/meta_upload.csv'

def hash_value(val):
    if val:
        return hashlib.sha256(val.strip().lower().encode()).hexdigest()
    return ''

seen_ids = set()

with open(INPUT_PATH, 'r', newline='', encoding='utf-8') as infile, \
     open(OUTPUT_PATH, 'w', newline='', encoding='utf-8') as outfile:

    reader = csv.DictReader(infile)
    fieldnames = ['email', 'phone', 'fn', 'ln', 'zip', 'ct', 'st', 'external_id']
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        # Skip voided or suspended tickets
        if row.get('TicketVoided') == 'TRUE' or row.get('TicketSuspended') == 'TRUE':
            continue

        # Filter by non-zero spend or orders
        if float(row.get('orders', 0)) == 0 and float(row.get('total_spent', 0)) == 0:
            continue

        # Deduplicate on email or rics_id
        uid = row.get('email', '').lower() or row.get('rics_id', '')
        if uid in seen_ids:
            continue
        seen_ids.add(uid)

        writer.writerow({
            'email': hash_value(row.get('email')),
            'phone': hash_value(row.get('phone')),
            'fn': hash_value(row.get('first_name')),
            'ln': hash_value(row.get('last_name')),
            'zip': hash_value(row.get('zip')),
            'ct': hash_value(row.get('city')),
            'st': hash_value(row.get('state')),
            'external_id': hash_value(row.get('rics_id'))
        })

print(f"Meta upload CSV written to {OUTPUT_PATH}")
