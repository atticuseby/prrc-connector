from pathlib import Path

# Create the full Python Playwright script content
script_path = Path("scripts/download_participants_csv.py")
script_path.parent.mkdir(parents=True, exist_ok=True)

script_code = '''
import os
import time
from playwright.sync_api import sync_playwright

# Load credentials from environment variables
RSU_USERNAME = os.getenv("RSU_USERNAME")
RSU_PASSWORD = os.getenv("RSU_PASSWORD")

# Participant export URL (adjust partner ID if needed)
EXPORT_URL = "https://runsignup.com/Partner/Participants/Report/1385"

# Download directory
DOWNLOAD_DIR = os.path.abspath("optimizely_connector/output")

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        print("🔐 Navigating to login...")
        page.goto("https://runsignup.com/Login")

        page.fill('input[name="email"]', RSU_USERNAME)
        page.fill('input[name="password"]', RSU_PASSWORD)
        page.click('button:has-text("Login")')

        page.wait_for_url("https://runsignup.com/MyDashboard", timeout=10000)
        print("✅ Logged in!")

        print(f"🌐 Navigating to {EXPORT_URL}...")
        page.goto(EXPORT_URL)

        print("📥 Clicking Export CSV...")
        with page.expect_download() as download_info:
            page.click("button:has-text('Export to CSV')")
        download = download_info.value

        file_path = os.path.join(DOWNLOAD_DIR, download.suggested_filename)
        download.save_as(file_path)
        print(f"✅ Downloaded CSV to {file_path}")

        browser.close()

if __name__ == "__main__":
    run()
'''

script_path.write_text(script_code.strip())
import ace_tools as tools; tools.display_dataframe_to_user(name="📁 Script Created", dataframe=None)
