# scripts/get_events_for_race.py

import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("RUNSIGNUP_API_KEY")
API_SECRET = os.getenv("RUNSIGNUP_API_SECRET")
BASE_URL = "https://runsignup.com/Rest"

race_id = "173294"

print(f"🔍 Getting events for race {race_id}...")

url = f"{BASE_URL}/race/{race_id}?format=json"
params = {
    "api_key": API_KEY,
    "api_secret": API_SECRET
}

response = requests.get(url, params=params)
data = response.json()

if "race" not in data:
    print("❌ Failed to retrieve race data.")
    print(data)
    exit()

race_info = data["race"]
events = race_info.get("events", [])

print(f"📦 Found {len(events)} event(s) in race {race_id}:\n")
for e in events:
    print(f"🆔 Event ID: {e['event_id']}")
    print(f"📛 Event Name: {e['name']}\n")
