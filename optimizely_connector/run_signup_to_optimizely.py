import os
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = "https://runsignup.com/Rest"
OUTPUT_PATH = f"data/runsignup_export_{datetime.now().strftime('%Y-%m-%d')}.csv"

def fetch_runsignup_data():
    test_race_id = "173466"  # Ashburn Village Fiesta Run
    race_name = "Soldier Run"

    print(f"\n📥 Testing known race: {race_name} (ID: {test_race_id})")

    reg_response = requests.get(
        f"{BASE_URL}/race/{test_race_id}/registrations",
        params={
            "api_key": API_KEY,
            "api_secret": API_SECRET,
            "format": "json"
        }
    )

    if reg_response.status_code != 200:
        print(f"⚠️ Error fetching race {test_race_id}: {reg_response.status_code}")
        return

    registrations = reg_response.json().get("registrations", [])
    print(f"✅ Found {len(registrations)} registrations")

    for reg in registrations[:2]:
        print(f"🧍 {reg.get('first_name')} {reg.get('last_name')} - {reg.get('email')}")

def fetch_events_and_registrations():
    print("(Skipping fetch_events_and_registrations – hardcoded test mode)")
