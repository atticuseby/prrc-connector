import os
import requests
import csv
from dotenv import load_dotenv
from scripts.helpers import log_message

load_dotenv()

API_KEY = os.getenv("RUNSIGNUP_API_KEY")
API_SECRET = os.getenv("RUNSIGNUP_API_SECRET")
PARTNER_ID = os.getenv("RUNSIGNUP_PARTNER_ID")

# Updated to use rsu_api_key param
BASE_URL = "https://runsignup.com/rest/races?format=json&events=T&event_type=running_race"
if PARTNER_ID:
    BASE_URL += f"&partner_id={PARTNER_ID}"

def extract_event_ids():
    print("🔍 Requesting race and event data from RunSignUp...")

    try:
        response = requests.get(
            BASE_URL,
            params={
                "rsu_api_key": API_KEY,
                "results_per_page": 100
            },
            headers={
                "X-RSU-API-SECRET": API_SECRET
            }
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log_message(f"❌ Failed to fetch races from RunSignUp: {e}")
        return

    data = response.json()
    print("📄 Full RunSignUp response:")
    print(data)

    races = data.get("races", [])
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
            r = race.get("race", {})
            if r.get("is_draft_race", False):
                continue

            for event in r.get("events", []):
                writer.writerow({
                    "race_id": r.get("race_id"),
                    "race_name": r.get("name"),
                    "event_id": event.get("event_id"),
                    "event_name": event.get("name")
                })

    print("✅ Event IDs extracted")
