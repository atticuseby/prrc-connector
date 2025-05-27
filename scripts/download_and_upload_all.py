# scripts/download_and_upload_all.py

from scripts.download_participants_csv import main as download_main
from scripts.save_cookies import save_cookies

# Call `save_cookies()` once manually before running this.
# Then run this daily via cron or GitHub Actions.

def run_download_pipeline():
    print("🚀 Starting RunSignUp CSV Pull + Upload pipeline")
    download_main()
    print("✅ All partner CSVs downloaded and uploaded")

if __name__ == "__main__":
    run_download_pipeline()
