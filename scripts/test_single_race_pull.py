import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("RUNSIGNUP_API_KEY")
API_SECRET = os.getenv("RUNSIGNUP_API_SECRET")
RACE_ID = "173294"

url = f"https://runsignup.com/Rest/race/{RACE_ID}/registrations"
print(f"📡 Hitting URL: {url}")

response = requests.get(
    url,
    params={"rsu_api_key": API_KEY, "format": "json"},
    headers={"X-RSU-API-SECRET": API_SECRET}
)

try:
    response.raise_for_status()
    data = response.json()
    print(f"✅ Found {len(data.get('registrations', []))} registrations")
except requests.RequestException as e:
    print(f"❌ Request failed: {e}")
    print("📄 Response content:", response.text)
