# optimizely_connector/upload_to_gdrive.py

import os
import glob
import io
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# 🔐 Load credentials from service_account.json
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive.file']

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# 📁 Replace with your actual folder ID if you want to upload to a specific folder
FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")
if not FOLDER_ID:
    print("⚠️ No folder ID set — uploading to root")

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

    for csv_path in glob.glob("**/*.csv", recursive=True):
        print(f"📤 Uploading {csv_path}...")
        upload_file(csv_path, service)

if __name__ == '__main__':
    main()
