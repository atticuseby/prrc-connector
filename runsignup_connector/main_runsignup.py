import os
from dotenv import load_dotenv
from runsignup_connector.run_signup_to_optimizely import fetch_runsignup_data
from runsignup_connector.upload_to_gdrive import upload_to_drive

load_dotenv()

def run_all():
    print("=== RUNSIGNUP CONNECTOR START ===")

    # Pull and sync registration data for all partners
    fetch_runsignup_data()

    # Upload to Google Drive if export exists
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    filepath = f"optimizely_connector/output/runsignup_export_{today}.csv"

    if os.path.exists(filepath):
        upload_to_drive(filepath)
    else:
        print(f"❌ RunSignUp export not found: {filepath}")

    print("=== RUNSIGNUP CONNECTOR END ===")

if __name__ == "__main__":
    run_all()
