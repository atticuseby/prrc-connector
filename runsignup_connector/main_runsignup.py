# runsignup_connector/main_runsignup.py

import os
import csv
import requests
from dotenv import load_dotenv
from datetime import datetime
from extract_event_ids import extract_event_ids
from run_signup_to_optimizely import fetch_runsignup_data
from scripts.upload_to_gdrive import upload_to_drive

load_dotenv()

OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN")

def push_runsignup_to_optimizely(csv_path):
    if not os.path.exists(csv_path):
        print(f"❌ RunSignUp export not found: {csv_path}")
        return

    print("📤 Preparing to push RunSignUp profiles to Optimizely...")

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        profiles = []

        for row in reader:
            email = row.get("email", "").strip().lower()
            if not email:
                continue
            profile = {
                "email": email,
                "first_name": row.get("first_name", ""),
                "last_name": row.get("last_name", ""),
                "event_id": row.get("event_id", ""),
                "race_id": row.get("race_id", "")
            }
            profiles.append({"attributes": profile})

    if not profiles:
        print("⚠️ No valid profiles to push.")
        return

    headers = {
        "Authorization": f"Bearer {OPTIMIZELY_API_TOKEN}",
        "Content-Type": "application/json"
    }

    response = requests.post(
        "https://api.zaius.com/v3/profiles",
        headers=headers,
        json={"profiles": profiles}
    )

    if response.status_code == 202:
        print("✅ RunSignUp profiles accepted by Optimizely (202)")
    else:
        print(f"❌ Optimizely API error {response.status_code}: {response.text}")

def run_runsignup_flow():
    print("=== RUNSIGNUP CONNECTOR START ===")

    try:
        extract_event_ids()
        print("✅ Event IDs extracted\n")
    except Exception as e:
        print(f"❌ Failed to extract event IDs: {e}\n")

    try:
        fetch_runsignup_data()
        print("✅ Registration data fetched\n")
    except Exception as e:
        print(f"❌ Failed to fetch registration data: {e}\n")

    today = datetime.now().strftime("%Y-%m-%d")
    output_file = f"optimizely_connector/output/runsignup_export_{today}.csv"

    try:
        upload_to_drive(local_file_path=output_file, drive_subfolder="RunSignUp")
        print("✅ Uploaded RunSignUp export to Google Drive\n")
    except Exception as e:
        print(f"❌ Google Drive upload failed: {e}\n")

    try:
        push_runsignup_to_optimizely(csv_path=output_file)
        print("✅ Optimizely sync complete\n")
    except Exception as e:
        print(f"❌ Optimizely sync failed: {e}\n")

    print("=== RUNSIGNUP CONNECTOR END ===")

if __name__ == "__main__":
    run_runsignup_flow()
