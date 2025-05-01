import os
import csv
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
PARTNER_ID = os.getenv("PARTNER_ID")  # ← This must be set in Railway
BASE_URL = "https://runsignup.com/Rest"
OUTPUT_PATH = f"data/runsignup_export_{datetime.now().strftime('%Y-%m-%d')}.csv"


def get_all_races():
    response = requests.get(
        f"{BASE_URL}/races",
        params={
            "api_key": API_KEY,
            "api_secret": API_SECRET,
            "partner_id": PARTNER_ID,
            "format": "json"
        }
    )

    if response.status_code != 200:
        print(f"⚠️ Error fetching race list: {response.status_code}")
        return []

    races_raw = response.json().get("races", [])
    races = [r["race"] for r in races_raw if "race" in r]

    # Log a preview
    for r in races[:5]:
        print(f"📍 Found: {r.get('name')} (ID: {r.get('race_id')})")

    return races


def fetch_events_and_registrations():
    races = get_all_races()
    for race in races:
        race_id = race.get("race_id")
        race_name = race.get("name")
        print(f"\n📌 {race_name} (ID: {race_id})")

        event_url = f"{BASE_URL}/race/{race_id}/events"
        event_response = requests.get(
            event_url,
            params={
                "api_key": API_KEY,
                "api_secret": API_SECRET,
                "format": "json"
            }
        )

        if event_response.status_code != 200:
            print(f"⚠️ Error fetching events: {event_response.status_code}")
            continue

        events = event_response.json().get("events", [])
        for event in events:
            event_id = event.get("event_id")
            event_name = event.get("name")
            print(f"  🏁 Event: {event_name} (ID: {event_id})")

            reg_response = requests.get(
                f"{BASE_URL}/race/{race_id}/registrations",
                params={
                    "event_id": event_id,
                    "api_key": API_KEY,
                    "api_secret": API_SECRET,
                    "format": "json"
                }
            )

            if reg_response.status_code != 200:
                print(f"   ⚠️ Error fetching registrations: {reg_response.status_code}")
                continue

            registrations = reg_response.json().get("registrations", [])
            print(f"   ✅ {len(registrations)} registrations found")


def fetch_runsignup_data():
    races = get_all_races()
    os.makedirs("data", exist_ok=True)

    with open(OUTPUT_PATH, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[
            "run_signup_id", "first_name", "last_name", "email", "phone",
            "date_of_birth", "address", "city", "state", "zip",
            "gender", "event_name", "registration_date", "transaction_id",
            "amount_paid", "checked_in", "source_race"
        ])
        writer.writeheader()

        for race in races:
            race_id = race.get("race_id")
            race_name = race.get("name")

            print(f"Fetching registrations for race: {race_name} (ID: {race_id})")

            reg_response = requests.get(
                f"{BASE_URL}/race/{race_id}/registrations",
                params={
                    "api_key": API_KEY,
                    "api_secret": API_SECRET,
                    "format": "json"
                }
            )

            if reg_response.status_code != 200:
                print(f"⚠️ Error fetching race {race_id}: {reg_response.status_code}")
                continue

            registrations = reg_response.json().get("registrations", [])

            for reg in registrations:
                writer.writerow({
                    "run_signup_id": reg.get("registration_id"),
                    "first_name": reg.get("first_name"),
                    "last_name": reg.get("last_name"),
                    "email": reg.get("email"),
                    "phone": reg.get("phone"),
                    "date_of_birth": reg.get("dob"),
                    "address": reg.get("address"),
                    "city": reg.get("city"),
                    "state": reg.get("state"),
                    "zip": reg.get("zip"),
                    "gender": reg.get("gender"),
                    "event_name": reg.get("event_name"),
                    "registration_date": reg.get("registration_date"),
                    "transaction_id": reg.get("transaction_id"),
                    "amount_paid": reg.get("amount_paid"),
                    "checked_in": reg.get("checked_in"),
                    "source_race": race_name
                })

    print(f"\n✅ RunSignUp data export complete: {OUTPUT_PATH}")
