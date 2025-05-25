# upload_to_gdrive.py

import os
import json
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime

SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# 🔧 Your top-level shared drive folder ID (replace if needed)
PARENT_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

def upload_to_drive(local_file_path, drive_subfolder="RICS"):
    """Uploads a file to a specific subfolder in Google Drive."""
    print(f"📤 Uploading {local_file_path} to Google Drive → {drive_subfolder}/")

    if not os.path.exists(local_file_path):
        print(f"❌ File not found: {local_file_path}")
        return

    # Authenticate
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    drive_service = build("drive", "v3", credentials=creds)

    # 🔍 Step 1: Find or create the subfolder
    query = (
        f"'{PARENT_FOLDER_ID}' in parents and name='{drive_subfolder}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])

    if items:
        folder_id = items[0]["id"]
        print(f"📁 Found existing folder: {drive_subfolder} (ID: {folder_id})")
    else:
        file_metadata = {
            "name": drive_subfolder,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [PARENT_FOLDER_ID],
        }
        folder = drive_service.files().create(body=file_metadata, fields="id").execute()
        folder_id = folder.get("id")
        print(f"📁 Created new folder: {drive_subfolder} (ID: {folder_id})")

    # 📄 Step 2: Upload file
    from googleapiclient.http import MediaFileUpload

    file_name = os.path.basename(local_file_path)
    media = MediaFileUpload(local_file_path, resumable=True)
    file_metadata = {"name": file_name, "parents": [folder_id]}

    uploaded_file = drive_service.files().create(
        body=file_metadata, media_body=media, fields="id"
    ).execute()

    print(f"✅ File uploaded to Drive with ID: {uploaded_file.get('id')}")
