import os
import sys
import logging
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2 import service_account

# === Setup logging ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# === Environment ===
GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS")
GDRIVE_FOLDER_ID_RICS = os.getenv("GDRIVE_FOLDER_ID_RICS")

if not GDRIVE_CREDENTIALS or not GDRIVE_FOLDER_ID_RICS:
    logging.error("Missing Google Drive credentials or folder ID")
    sys.exit(1)

# === Build Drive Service ===
try:
    creds = service_account.Credentials.from_service_account_info(
        eval(GDRIVE_CREDENTIALS),
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    drive_service = build("drive", "v3", credentials=creds)
except Exception as e:
    logging.error(f"Failed to initialize Google Drive service: {e}")
    sys.exit(1)

def upload_to_drive(file_path, filename=None):
    """
    Upload file to Google Drive folder.
    - If file with same name exists, overwrite it.
    - Otherwise, create a new file.
    """
    if not filename:
        filename = os.path.basename(file_path)

    if not os.path.exists(file_path):
        logging.error(f"File does not exist: {file_path}")
        return

    logging.info(f"Uploading {file_path} as {filename}")

    try:
        # Step 1: Search for existing file in folder
        query = f"name='{filename}' and '{GDRIVE_FOLDER_ID_RICS}' in parents and trashed=false"
        results = drive_service.files().list(q=query, spaces="drive", fields="files(id, name)").execute()
        files = results.get("files", [])

        media = MediaFileUpload(file_path, resumable=True)

        if files:
            # Overwrite existing file
            file_id = files[0]["id"]
            logging.info(f"Overwriting existing file {filename} (id={file_id})")
            drive_service.files().update(fileId=file_id, media_body=media).execute()
        else:
            # Create new file
            logging.info(f"No existing {filename}. Creating new file.")
            file_metadata = {"name": filename, "parents": [GDRIVE_FOLDER_ID_RICS]}
            drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()

        logging.info(f"✅ Uploaded {filename} to Google Drive")

    except Exception as e:
        logging.error(f"Error uploading {filename}: {e}")
        raise
