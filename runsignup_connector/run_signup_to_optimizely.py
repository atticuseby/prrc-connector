# runsignup_connector/run_signup_to_optimizely.py

import os
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("RUNSIGNUP_API_KEY")
API_SECRET = os.getenv("RUNSIGNUP_API_SECRET")
PARTNER_IDS = os.getenv("RUNSIGNUP_PARTNER_IDS", "").split(",")

print(f"🔢 Partner IDs loaded from env: {PARTNER_IDS}")

today = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = "optimizely_connector/output"
OUTPUT_PATH = f"{OUTPUT_DIR}/runsignup_export_{today}.csv"

BASE_URL = "https://runsignup.com/Rest"

def fetch_runsignup_data():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_regs = []

    for partner_id in PARTNER_IDS:
        partner_id = partner_id.strip()
        if not partner_id:
            print("⚠️ Skipping empty partner_id value")
            continue

        print(f"🔍 Pulling registrations for Partner ID: {partner_id}")

        url = f"{BASE_URL}/partners/{partner_id}/registrations"
        print(f"🌐 Request URL: {url}")

        try:
            response = requests.get(
                url,
                params={
                    "rsu_api_key": API_KEY,
                    "format": "json"
                },
                headers={
                    "X-RSU-API-SECRET": API_SECRET
                }
            )
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"❌ Request failed for partner {partner_id}: {e}")
            continue

        regs = response.json().get("registrations", [])
        if not regs:
            print(f"⚠️ No registrants found for partner {partner_id}")
            continue

        print(f"✅ Found {len(regs)} registrations for partner {partner_id}")

        for reg in regs:
            all_regs.append({
                "first_name": reg.get("first_name", ""),
                "middle_name": reg.get("middle_name", ""),
                "last_name": reg.get("last_name", ""),
                "email": reg.get("email", ""),
                "gender": reg.get("gender", ""),
                "age": reg.get("age", ""),
                "race": reg.get("race_name", ""),
                "event": reg.get("event_name", ""),
                "event_year": reg.get("event_year", ""),
                "registration_date": reg.get("registration_date", ""),
                "partner_id": partner_id
            })

    if not all_regs:
        print("⚠️ No registrations found — no file written.")
        return

    print(f"💾 Writing {len(all_regs)} registrations to {OUTPUT_PATH}")

    with open(OUTPUT_PATH, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "first_name", "middle_name", "last_name", "email", "gender", "age",
            "race", "event", "event_year", "registration_date", "partner_id"
        ])
        writer.writeheader()
        for row in all_regs:
            writer.writerow(row)
