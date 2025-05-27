# optimizely_connector/main.py

import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
from rics_connector.fetch_rics_data import fetch_rics_data
from rics_connector.sync_rics_to_optimizely import run_sync
from scripts.upload_to_gdrive import upload_to_drive

load_dotenv()

def run_rics_flow():
    dry_run = os.getenv("DRY_RUN", "true").lower() == "true"
    if not dry_run:
        print("🟢 DRY_RUN is OFF — this run will push data to Optimizely.")
        print("🚨 LIVE DATA MODE — Check Optimizely after run.\n")
    else:
        print("🧪 DRY_RUN is ON — no data will be sent.\n")

    print("=== RICS CONNECTOR START ===")

    try:
        print("📥 Pulling RICS data...")
        fetch_rics_data()
        print("✅ RICS data pull complete\n")
    except Exception as e:
        print(f"❌ RICS data pull error: {e}\n")

    today = datetime.now().strftime("%Y-%m-%d")
    output_file = f"optimizely_connector/output/rics_export_{today}.csv"

    try:
        upload_to_drive(local_file_path=output_file, drive_subfolder="RICS")
        print("✅ Uploaded RICS export to Google Drive\n")
    except Exception as e:
        print(f"❌ RICS Google Drive upload failed: {e}\n")

    if not dry_run:
        try:
            print("🚀 Pushing RICS data to Optimizely...")
            run_sync()
            print("✅ RICS Optimizely sync complete\n")
        except Exception as e:
            print(f"❌ RICS Optimizely sync failed: {e}\n")

    print("=== RICS CONNECTOR END ===")

if __name__ == "__main__":
    run_rics_flow()
