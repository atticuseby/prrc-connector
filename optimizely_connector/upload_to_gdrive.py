# optimizely_connector/upload_to_gdrive.py

import os
import glob
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 🔐 Load credentials from service_account.json
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive.file']

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# 📁 Get target folder ID from environment
FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()

if FOLDER_ID:
    print(f"📂 Target Drive folder: {FOLDER_ID}")
else:
    print("⚠️ No GDRIVE_FOLDER_ID set — uploading to root folder")

def upload_file(filepath, drive_service):
    file_metadata = {
        'name': os.path.basename(filepath)
    }
    if FOLDER_ID:
        file_metadata['parents'] = [FOLDER_ID]

    media = MediaFileUpload(filepath, mimetype='text/csv')
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id'
    ).execute()

    print(f"✅ Uploaded {filepath} as file ID: {file.get('id')}")

def main():
    service = build('drive', 'v3', credentials=creds)

    csv_files = glob.glob("**/*.csv", recursive=True)
    if not csv_files:
        print("⚠️ No CSV files found to upload.")
        return

    for csv_path in csv_files:
        print(f"📤 Uploading {csv_path}...")
        upload_file(csv_path, service)

if __name__ == '__main__':
    main()
