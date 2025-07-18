import csv
import sys
import os
from collections import OrderedDict

INPUT_PATH = sys.argv[1] if len(sys.argv) > 1 else 'data/rics_customer_purchase_history_latest.csv'
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else 'data/rics_customers_deduped.csv'

# Fields to keep for customer profile
customer_fields = [
    "rics_id", "email", "first_name", "last_name", "orders", "total_spent", "city", "state", "zip", "phone"
]

def deduplicate_customers(input_path, output_path):
    customers = OrderedDict()
    with open(input_path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cust_id = row["rics_id"]
            if cust_id and cust_id not in customers:
                customers[cust_id] = {field: row.get(field, "") for field in customer_fields}
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=customer_fields)
        writer.writeheader()
        writer.writerows(customers.values())
    print(f"✅ Wrote deduplicated customer CSV: {output_path} ({len(customers)} unique customers)")

if __name__ == "__main__":
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Input file not found: {INPUT_PATH}")
        sys.exit(1)
    deduplicate_customers(INPUT_PATH, OUTPUT_PATH) 