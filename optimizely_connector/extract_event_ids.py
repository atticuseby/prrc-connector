# extract_event_ids.py

import os
import requests
import csv
from scripts.helpers import log_message
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
PARTNER_ID = os.getenv("PARTNER_ID")

RUNSIGNUP_RACES_ENDPOINT = f"https://runsignup.com/rest/races?format=json&event_type=R&api_key={API_KEY}&api_secret={API_SECRET}&partner_id={PARTNER_ID}"

def extract_event_ids():
    print("🔍 Requesting race and event data from RunSignUp...")
    try:
        response = requests.get(RUNSIGNUP_RACES_ENDPOINT)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log_message(f"❌ Failed to fetch races from RunSignUp: {e}")
        return

    races = response.json().get("races", [])
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
                writer.writerow({
                    "race_id": race_id,
                    "race_name": race_name,
                    "event_id": event.get("event_id"),
                    "event_name": event.get("name")
                })

    log_message(f"✅ Saved event IDs to {output_path}")
