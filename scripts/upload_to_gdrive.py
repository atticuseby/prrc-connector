import os
import sys
import json
import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def _get_drive_service_and_folder():
    creds_json = os.getenv("GDRIVE_CREDENTIALS")
    folder_id = os.getenv("GDRIVE_FOLDER_ID_RICS")

    missing = []
    if not creds_json:
        missing.append("GDRIVE_CREDENTIALS")
    if not folder_id:
        missing.append("GDRIVE_FOLDER_ID_RICS")
    if missing:
        raise RuntimeError(f"Missing env: {', '.join(missing)}")

    try:
        info = json.loads(creds_json)
    except Exception as e:
        raise RuntimeError(f"GDRIVE_CREDENTIALS is not valid JSON: {e}")

    try:
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=creds)
    except Exception as e:
        raise RuntimeError(f"Failed to initialize Google Drive service: {e}")

    return service, folder_id

def upload_to_drive(file_path, filename=None):
    """
    Upload file to Google Drive folder.
    - If a file with the same name exists in the folder, overwrite it.
    - Otherwise, create a new file.
    """
    if not filename:
        filename = os.path.basename(file_path)

    if not os.path.exists(file_path):
        logging.error(f"File does not exist: {file_path}")
        return

    service, folder_id = _get_drive_service_and_folder()

    logging.info(f"Uploading {file_path} as {filename}")
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"

    try:
        existing = service.files().list(q=query, spaces="drive", fields="files(id,name)").execute().get("files", [])
        media = MediaFileUpload(file_path, resumable=True)

        if existing:
            file_id = existing[0]["id"]
            logging.info(f"Overwriting existing file {filename} (id={file_id})")
            service.files().update(fileId=file_id, media_body=media).execute()
        else:
            logging.info(f"No existing {filename}. Creating new file.")
            metadata = {"name": filename, "parents": [folder_id]}
            service.files().create(body=metadata, media_body=media, fields="id").execute()

        logging.info(f"✅ Uploaded {filename} to Google Drive")
    except Exception as e:
        logging.error(f"Error uploading {filename}: {e}")
        raise
