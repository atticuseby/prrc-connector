import requests
import csv
from datetime import datetime
from dotenv import load_dotenv
import os

# --- CONFIG ---
load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
OUTPUT_PATH = f"data/runsignup_export_{datetime.now().strftime('%Y-%m-%d')}.csv"
BASE_URL = "https://runsignup.com/Rest"

def fetch_runsignup_data():
    print("\n=== Pulling RunSignUp Data ===\n")

    # --- STEP 1: GET ALL RACES ---
    races_response = requests.get(
        f"{BASE_URL}/races",
        params={
            "api_key": API_KEY,
            "api_secret": API_SECRET,
            "format": "json"
        }
    )

    try:
        races_data = races_response.json()
        print(f"📦 Raw races response:\n{races_data}")
        race_list = races_data.get("races", [])
    except Exception as e:
        print(f"❌ Error parsing races response: {e}")
        return

    # --- STEP 2: PREP CSV ---
    os.makedirs("data", exist_ok=True)  # Ensure 'data' folder exists

    with open(OUTPUT_PATH, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[
            "run_signup_id", "first_name", "last_name", "email", "phone",
            "date_of_birth", "address", "city", "state", "zip",
            "gender", "event_name", "registration_date", "transaction_id",
            "amount_paid", "checked_in", "source_race"
        ])
        writer.writeheader()

        # --- STEP 3: LOOP THROUGH EACH RACE ---
        for race in race_list:
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

            reg_data = reg_response.json()
            registrations = reg_data.get("registrations", [])

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

    # TEMP: Preview first row if available
    try:
        with open(OUTPUT_PATH, newline='') as f:
            reader = csv.DictReader(f)
            first_row = next(reader)
            print(f"\n🧪 First row of CSV:\n{first_row}")
    except StopIteration:
        print("⚠️ No rows found in CSV.")
