# main_runsignup.py

import os
from dotenv import load_dotenv
from extract_event_ids import extract_event_ids
from run_signup_to_optimizely import fetch_runsignup_data
from rics_connector.upload_to_gdrive import upload_to_drive  # reuse from RICS for now

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

    try:
        # 🔄 Upload to Google Drive → RunSignUp folder
        output_file = "optimizely_connector/output/runsignup_export_2025-05-25.csv"  # use dynamic path if needed
        upload_to_drive(local_file_path=output_file, drive_subfolder="RunSignUp")
        print("✅ Uploaded to Google Drive\n")
    except Exception as e:
        print(f"❌ Google Drive upload failed: {e}\n")

    print("=== RUNSIGNUP CONNECTOR END ===")

if __name__ == "__main__":
    run_runsignup_flow()
