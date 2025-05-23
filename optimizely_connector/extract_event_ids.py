# extract_event_ids.py

import os
import sys
import requests
import csv

# ✅ Allow import from scripts/helpers.py
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))
from helpers import log_message

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
PARTNER_ID = os.getenv("PARTNER_ID")

RUNSIGNUP_RACES_ENDPOINT = f"https://runsignup.com/rest/races?format=json&events=T&event_type=R&api_key={API_KEY}&api_secret={API_SECRET}&partner_id={PARTNER_ID}"

def extract_event_ids():
    print("🔍 Requesting race and event data from RunSignUp...")

    try:
        response = requests.get(RUNSIGNUP_RACES_ENDPOINT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log_message(f"❌ Failed to fetch races from RunSignUp: {e}")
        return

    races = response.json().get("races", [])
    if not races:
        print("⚠️ No races returned from RunSignUp")
        return

    print(f"📦 Found {len(races)} races")

    output_path = "data/event_ids.csv"
    os.makedirs("data", exist_ok=True)

    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=["race_id", "race_name", "event_id", "event_name"])
        writer.writeheader()

        for race in races:
            race_id = race.get("race_id")
            race_name = race.get("name")

            for event in race.get("events", []):
                event_id = event.get("event_id")
                event_name = event.get("name")

                # 🪵 Print to logs for debugging
                print(f"{race_name} ({race_id}) → {event_name} ({event_id})")

                writer.writerow({
                    "race_id": race_id,
                    "race_name": race_name,
                    "event_id": event_id,
                    "event_name": event_name
                })

    log_message(f"✅ Saved event IDs to {output_path}")

# ✅ Make sure this runs when the script is called directly
if __name__ == "__main__":
    extract_event_ids()
