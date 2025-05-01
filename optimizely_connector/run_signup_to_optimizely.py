import requests
import os

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = "https://runsignup.com/Rest"

def fetch_events_and_registrations(race_list):
    for race in race_list:
        race_id = race.get("race_id")
        race_name = race.get("name")

        # === GET EVENT DETAILS FOR EACH RACE ===
        detail_resp = requests.get(
            f"{BASE_URL}/race/{race_id}",
            params={
                "api_key": API_KEY,
                "api_secret": API_SECRET,
                "format": "json"
            }
        )

        if detail_resp.status_code != 200:
            print(f"⚠️ Error fetching details for race {race_id}")
            continue

        race_data = detail_resp.json()
        events = race_data.get("race", {}).get("events", [])

        for event in events:
            event_id = event.get("event_id")
            event_name = event.get("name")

            print(f"\nFetching registrations for race: {race_name} — Event: {event_name} (ID: {event_id})")

            reg_resp = requests.get(
                f"{BASE_URL}/race/{race_id}/event/{event_id}/registrations",
                params={
                    "api_key": API_KEY,
                    "api_secret": API_SECRET,
                    "format": "json"
                }
            )

            if reg_resp.status_code != 200:
                print(f"⚠️ Error fetching registrations for event {event_id}: {reg_resp.status_code}")
                continue

            reg_data = reg_resp.json()
            registrations = reg_data.get("registrations", [])

            if not registrations:
                print("⚠️ No registrations found.")
            else:
                print(f"✅ Found {len(registrations)} registrations.")
                # Here’s where you’d save or process the data
