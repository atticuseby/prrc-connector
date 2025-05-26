import os
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("RUNSIGNUP_API_KEY")
API_SECRET = os.getenv("RUNSIGNUP_API_SECRET")
BASE_URL = "https://runsignup.com/Rest"
EVENT_IDS_PATH = "data/event_ids.csv"

today = datetime.now().strftime("%Y-%m-%d")
OUTPUT_DIR = "optimizely_connector/output"
OUTPUT_PATH = f"{OUTPUT_DIR}/runsignup_export_{today}.csv"

def fetch_runsignup_data():
    if not os.path.exists(EVENT_IDS_PATH):
        print("⚠️ event_ids.csv not found. Skipping RunSignUp pull.")
        return

    print("📥 Loading event IDs from event_ids.csv...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    all_regs = []
    with open(EVENT_IDS_PATH, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            event_id = row["event_id"]
            race_id = row["race_id"]
            race_name = row["race_name"]

            print(f"🎯 Fetching registrations for {race_name} (Race ID: {race_id}, Event ID: {event_id})")

            try:
                response = requests.get(
                    f"{BASE_URL}/event/{event_id}/registrations",
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
                print(f"❌ Request failed for event {event_id}: {e}")
                continue

            regs = response.json().get("registrations", [])
            if not regs:
                print(f"⚠️ 0 registrants for event {event_id}")
                continue

            print(f"✅ Found {len(regs)} registrations")

            for reg in regs:
                reg["race_id"] = race_id
                reg["event_id"] = event_id
                all_regs.append(reg)

    if not all_regs:
        print("⚠️ No registrations found — no file written.")
        return

    print(f"💾 Writing {len(all_regs)} registrations to {OUTPUT_PATH}")

    with open(OUTPUT_PATH, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=[
            "race_id", "event_id", "first_name", "last_name", "email"
        ])
        writer.writeheader()
        for r in all_regs:
            writer.writerow({
                "race_id": r.get("race_id", ""),
                "event_id": r.get("event_id", ""),
                "first_name": r.get("first_name", ""),
                "last_name": r.get("last_name", ""),
                "email": r.get("email", "")
            })
