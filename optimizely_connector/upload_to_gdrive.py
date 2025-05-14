# optimizely_connector/upload_to_gdrive.py

import os
import glob
import json
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

CREDENTIALS_PATH = "service_account.json"
UPLOAD_FOLDER_NAME = "PRRC Connector Uploads"  # this can be the folder's name

def authenticate():
    gauth = GoogleAuth()
    gauth.LoadServiceConfigSettings()
    gauth.ServiceAuth()
    return GoogleDrive(gauth)

def upload_csvs():
    drive = authenticate()

    for filepath in glob.glob("**/*.csv", recursive=True):
        print(f"📤 Uploading {filepath} to Google Drive...")
        file = drive.CreateFile({'title': os.path.basename(filepath)})
        file.SetContentFile(filepath)
        file.Upload()
        print(f"✅ Uploaded: {filepath}")

if __name__ == "__main__":
    upload_csvs()
