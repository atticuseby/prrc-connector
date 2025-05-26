import os
import time
import base64
import requests
from datetime import datetime
from urllib.parse import unquote
from upload_to_gdrive import upload_to_drive

DOWNLOAD_DIR = os.path.join(os.getcwd(), "optimizely_connector", "output")
PARTICIPANT_URL = "https://runsignup.com/Partner/Participants/Report/1385"
DEBUG_DUMP = os.path.join(DOWNLOAD_DIR, "debug_response.html")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def download_csv():
    print("📥 Downloading CSV directly via requests...")

    cookie_header = os.environ.get("RUNSIGNUP_FULL_COOKIE_HEADER")
    if not cookie_header:
        raise ValueError("❌ Missing RUNSIGNUP_FULL_COOKIE_HEADER")

    cookies = {}
    for pair in cookie_header.split("; "):
        if "=" in pair:
            k, v = pair.split("=", 1)
            cookies[k] = v

    csv_url = PARTICIPANT_URL.replace("Participants/Report", "Participants/ReportDownloadCSV")
    timestamp = datetime.now().strftime("%Y-%m-%d")
    final_filename = f"run_signup_export_{timestamp}.csv"
    final_path = os.path.join(DOWNLOAD_DIR, final_filename)

    response = requests.get(csv_url, cookies=cookies)

    if "text/csv" not in response.headers.get("Content-Type", ""):
        print("❌ Unexpected response. Dumping for debug...")
        with open(DEBUG_DUMP, "w", encoding="utf-8") as f:
            f.write(response.text)
        raise ValueError("❌ CSV download failed or returned unexpected content.")

    with open(final_path, "wb") as f:
        f.write(response.content)

    print(f"✅ File downloaded and saved as: {final_path}")
    return final_path

def main():
    print("🚀 Starting RunSignUp CSV pull...")
    try:
        csv_path = download_csv()
        upload_to_drive(csv_path)
        print("📤 Upload to Drive complete.")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        if os.path.exists(DEBUG_DUMP):
            with open(DEBUG_DUMP, "rb") as f:
                print(f"\n--- DEBUG HTML DUMP ---\n{f.read()[:500]}\n--- END DUMP ---\n")
        raise

if __name__ == "__main__":
    main()
