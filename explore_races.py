import requests
import os
from dotenv import load_dotenv

# Load credentials from .env
load_dotenv()

API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")

BASE_URL = "https://runsignup.com/Rest"

def explore_races():
    print("=== Exploring Races ===\n")
    response = requests.get(
        f"{BASE_URL}/races",
        params={
            "api_key": API_KEY,
            "api_secret": API_SECRET,
            "format": "json",
            "results_per_page": 5
        }
    )

    if response.status_code != 200:
        print(f"❌ Failed to fetch races. Status code: {response.status_code}")
        return

    data = response.json()
    races = data.get("races", [])

    if not races:
        print("⚠️ No races found.")
        return

    for race in races:
        r = race.get("race", {})
        print("Race Name:", r.get("name"))
        print("Race ID:", r.get("race_id"))
        print("City/State:", f"{r.get('city')}, {r.get('state')}")
        print("Partner ID:", r.get("partner_id"))
        print("Org Name:", r.get("organization"))
        print("Is Draft:", r.get("is_draft_race"))
        print("-" * 40)

if __name__ == "__main__":
    explore_races()
