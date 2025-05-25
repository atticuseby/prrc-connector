import os
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

SERVICE_ACCOUNT_FILE = "service_account.json"
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

# Top-level shared folder ID
PARENT_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()

def upload_to_drive(local_file_path, drive_subfolder="General"):
    """Upload a single CSV file to a subfolder inside a shared Google Drive folder."""
    if not os.path.exists(local_file_path):
        print(f"❌ File not found: {local_file_path}")
        return

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    drive_service = build("drive", "v3", credentials=creds)

    # Check or create subfolder inside shared folder
    query = (
        f"'{PARENT_FOLDER_ID}' in parents and name='{drive_subfolder}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    )
    response = drive_service.files().list(q=query, fields="files(id)").execute()
    folders = response.get("files", [])

    if folders:
        folder_id = folders[0]["id"]
        print(f"📁 Found folder '{drive_subfolder}' (ID: {folder_id})")
    else:
        metadata = {
            "name": drive_subfolder,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [PARENT_FOLDER_ID]
        }
        folder = drive_service.files().create(body=metadata, fields="id").execute()
        folder_id = folder["id"]
        print(f"📁 Created folder '{drive_subfolder}' (ID: {folder_id})")

    # Upload file
    file_metadata = {"name": os.path.basename(local_file_path), "parents": [folder_id]}
    media = MediaFileUpload(local_file_path, mimetype="text/csv")

    uploaded = drive_service.files().create(
        body=file_metadata, media_body=media, fields="id"
    ).execute()

    print(f"✅ Uploaded {os.path.basename(local_file_path)} → Google Drive ID: {uploaded.get('id')}")
