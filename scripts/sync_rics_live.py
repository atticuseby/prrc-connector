import os
import sys
import traceback
import shutil
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Add repo root to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from rics_connector.fetch_rics_data import fetch_rics_data_with_purchase_history

# === CONFIG ===
RICS_API_TOKEN = os.getenv("RICS_API_TOKEN")
GDRIVE_FOLDER_ID_RICS = os.getenv("GDRIVE_FOLDER_ID_RICS")
CREDS_PATH = "optimizely_connector/service_account.json"
LOG_DIR = "logs"

os.makedirs(LOG_DIR, exist_ok=True)

log_file_path = os.path.join(LOG_DIR, f"sync_log_{datetime.now().strftime('%m_%d_%Y_%H%M')}.log")

def log(msg):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{timestamp} {msg}"
    print(line, flush=True)
    with open(log_file_path, "a") as f:
        f.write(line + "\n")

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    return build("drive", "v3", credentials=creds)

def upload_to_drive(filepath, folder_id):
    service = get_drive_service()
    file_metadata = {"name": os.path.basename(filepath), "parents": [folder_id]}
    media = MediaFileUpload(filepath)
    uploaded = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    log(f"✅ Uploaded {os.path.basename(filepath)} to Drive as ID: {uploaded['id']}")

def deduplicate_csv(input_csv, output_csv):
    """Load CSV, drop duplicate customers by rics_id+email, save cleaned copy."""
    df = pd.read_csv(input_csv)
    before = len(df)
    df = df.drop_duplicates(subset=["rics_id", "email"], keep="last")
    after = len(df)
    df.to_csv(output_csv, index=False)
    log(f"🧹 Deduplicated customers: {before} → {after} rows (saved {output_csv})")
    return output_csv

def main():
    log("🔥 ENTERED MAIN FUNCTION (purchase history export)")
    log(f"RICS_API_TOKEN present? {'✅' if RICS_API_TOKEN else '❌'}")
    try:
        # Fetch RICS data with purchase history
        result = fetch_rics_data_with_purchase_history(return_summary=True)
        if isinstance(result, tuple):
            output_csv, summary = result
        else:
            output_csv, summary = result, "No summary returned"

        log(f"📊 Exported RICS customer purchase history to: {output_csv}")
        log(f"📊 Dedup summary → {summary}")

        # Ensure output dir exists
        output_dir = os.path.join("optimizely_connector", "output")
        os.makedirs(output_dir, exist_ok=True)

        # Latest raw
        latest_raw = os.path.join(output_dir, "rics_customer_purchase_history_latest.csv")
        shutil.copyfile(output_csv, latest_raw)
        log(f"📁 Copied raw export to: {latest_raw}")

        # Deduplicated file
        deduped_path = os.path.join(output_dir, "rics_customer_purchase_history_deduped.csv")
        deduplicate_csv(latest_raw, deduped_path)

        # Upload all to Drive
        upload_to_drive(output_csv, GDRIVE_FOLDER_ID_RICS)       # timestamped raw
        upload_to_drive(latest_raw, GDRIVE_FOLDER_ID_RICS)       # alias raw
        upload_to_drive(deduped_path, GDRIVE_FOLDER_ID_RICS)     # deduped

    except Exception as e:
        log(f"❌ Error during RICS data export: {e}")
        log(traceback.format_exc())

    # Always upload the log
    try:
        upload_to_drive(log_file_path, GDRIVE_FOLDER_ID_RICS)
    except Exception as e:
        print(f"❌ Failed to upload log file: {e}")

    log("✅ All done!")

if __name__ == "__main__":
    main()
