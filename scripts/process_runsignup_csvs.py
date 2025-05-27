# scripts/process_runsignup_csvs.py

import os
import csv
from datetime import datetime, timedelta
from scripts.optimizely import send_to_optimizely

def process_runsignup_csvs(local_dir="optimizely_connector/output"):
    now = datetime.now()
    cutoff = now - timedelta(hours=24)

    for file in os.listdir(local_dir):
        if not file.endswith(".csv") or "runsignup_export" not in file:
            continue

        filepath = os.path.join(local_dir, file)
        print(f"📂 Processing: {filepath}")

        with open(filepath, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    reg_time = datetime.strptime(row["registration_date"], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    print(f"⚠️ Invalid timestamp format: {row['registration_date']}")
                    continue

                if reg_time < cutoff:
                    continue

                payload = {
                    "identifiers": {
                        "email": row["email"]
                    },
                    "attributes": {
                        "first_name": row["first_name"],
                        "middle_name": row["middle_name"],
                        "last_name": row["last_name"],
                        "gender": row["gender"],
                        "age": int(row["age"]) if row["age"] else None,
                        "event": row["event"],
                        "event_year": int(row["event_year"]) if row["event_year"] else None,
                        "registration_date": row["registration_date"],
                        "race": row["race"]
                    }
                }

                send_to_optimizely(payload)
