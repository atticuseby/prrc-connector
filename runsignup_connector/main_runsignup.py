import os
import sys
from datetime import datetime

from dotenv import load_dotenv

# Add parent directory to sys.path so we can import from /scripts/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from runsignup_connector.run_signup_to_optimizely import fetch_runsignup_data
from scripts.upload_to_gdrive import upload_to_drive
from scripts.process_runsignup_csvs import process_runsignup_csvs

load_dotenv()

def run_all():
    print("=== RUNSIGNUP CONNECTOR START ===")

    # Step 1: Fetch and write registrant data from all partner accounts
    fetch_runsignup_data()

    # Step 2: Upload CSV to Google Drive if it was written
    today = datetime.now().strftime("%Y-%m-%d")
    filepath = f"optimizely_connector/output/runsignup_export_{today}.csv"

    if os.path.exists(filepath):
        upload_to_drive(filepath)
    else:
        print(f"❌ RunSignUp export not found: {filepath}")
        return

    # Step 3: Process CSV and push to Optimizely
    process_runsignup_csvs()

    print("=== RUNSIGNUP CONNECTOR END ===")

if __name__ == "__main__":
    run_all()
