import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

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

    # Determine folder
    if "runsignup_export" in filepath:
        folder_id = os.getenv("GDRIVE_FOLDER_ID_RUNSIGNUP", "").strip()
    elif "rics_export" in filepath:
        folder_id = os.getenv("GDRIVE_FOLDER_ID", "").strip()
    else:
        folder_id = ""

    file_metadata = {"name": os.path.basename(filepath)}
    if folder_id:
        file_metadata["parents"] = [folder_id]
        print(f"📁 Target folder ID: {folder_id}")
    else:
        print("⚠️ No folder ID found — uploading to root")

    media = MediaFileUpload(filepath, mimetype="text/csv")
    uploaded_file = service.files().create(
        body=file_metadata, media_body=media, fields="id"
    ).execute()

    print(f"✅ Uploaded file ID: {uploaded_file.get('id')}")
