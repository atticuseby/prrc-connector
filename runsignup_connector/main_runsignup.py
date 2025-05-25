# runsignup_connector/main_runsignup.py

import os
from dotenv import load_dotenv
from datetime import datetime
from extract_event_ids import extract_event_ids
from run_signup_to_optimizely import fetch_runsignup_data
from rics_connector.upload_to_gdrive import upload_to_drive
from sync_rics_to_optimizely import push_to_optimizely  # Assuming this is shared

load_dotenv()

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

    # Build filename from today's date
    today = datetime.now().strftime("%Y-%m-%d")
    output_file = f"optimizely_connector/output/runsignup_export_{today}.csv"

    try:
        upload_to_drive(local_file_path=output_file, drive_subfolder="RunSignUp")
        print("✅ Uploaded to Google Drive\n")
    except Exception as e:
        print(f"❌ Google Drive upload failed: {e}\n")

    try:
        print("🚀 Pushing RunSignUp profiles to Optimizely...")
        push_to_optimizely(output_file)  # You may need to wrap this call
        print("✅ Optimizely sync complete\n")
    except Exception as e:
        print(f"❌ Failed to push to Optimizely: {e}\n")

    print("=== RUNSIGNUP CONNECTOR END ===")

if __name__ == "__main__":
    run_runsignup_flow()
