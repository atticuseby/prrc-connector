import os
import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

def main():
    # Auth
    creds = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    drive = build("drive", "v3", credentials=creds)

    # List RICS folder
    folder_id = os.environ["GDRIVE_FOLDER_ID_RICS"]
    resp = drive.files().list(
        q=f"'{folder_id}' in parents and trashed = false",
        fields="files(id,name,createdTime)"
    ).execute()

    files = resp.get("files", [])
    if not files:
        raise SystemExit("❌ No files found in the RICS Drive folder!")

    # Pick newest
    latest = max(files, key=lambda f: f["createdTime"])
    file_id, name = latest["id"], latest["name"]
    print(f"➡️ Downloading latest file: {name}")

    # Download
    request = drive.files().get_media(fileId=file_id)
    with io.FileIO("data/rics.csv", mode="wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"   Download {int(status.progress() * 100)}%")

    print("✅ Downloaded to data/rics.csv")

if __name__ == "__main__":
    main()
