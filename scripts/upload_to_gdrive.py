import os
import csv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

PARTNER_FOLDER_MAP = {
    "1384": os.getenv("GDRIVE_FOLDER_ID_1384", ""),
    "1385": os.getenv("GDRIVE_FOLDER_ID_1385", ""),
    "1411": os.getenv("GDRIVE_FOLDER_ID_1411", "")
}

def upload_to_drive(filepath):
    print(f"📤 Uploading {filepath} to Google Drive...")

    creds_path = os.path.join("optimizely_connector", "service_account.json")
    if not os.path.exists(creds_path):
        print(f"❌ Credentials file not found: {creds_path}")
        return

    creds = service_account.Credentials.from_service_account_file(
        creds_path, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    service = build("drive", "v3", credentials=creds)

    filename = os.path.basename(filepath).lower()

    # Default: upload to fallback folder
    folder_id = ""

    # Handle RICS
    if "rics_export" in filename:
        folder_id = os.getenv("GDRIVE_FOLDER_ID_RICS", "").strip()

    # Handle RunSignUp with partner-aware logic
    elif "runsignup_export" in filename:
        partner_id = None
        with open(filepath, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                partner_id = row.get("partner_id", "").strip()
                if partner_id:
                    break

        if partner_id and partner_id in PARTNER_FOLDER_MAP:
            folder_id = PARTNER_FOLDER_MAP[partner_id]
            print(f"📁 Detected partner_id: {partner_id} → uploading to folder ID: {folder_id}")
        else:
            print(f"⚠️ Could not detect partner_id in CSV — uploading to fallback folder")

    # Upload to Drive
    file_metadata = {"name": os.path.basename(filepath)}
    if folder_id:
        file_metadata["parents"] = [folder_id]

    media = MediaFileUpload(filepath, mimetype="text/csv")
    uploaded_file = service.files().create(
        body=file_metadata, media_body=media, fields="id"
    ).execute()

    print(f"✅ Uploaded file ID: {uploaded_file.get('id')}")
