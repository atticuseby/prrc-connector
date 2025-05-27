# scripts/download_all_runsignup_csvs.py

import os
import time
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scripts.upload_to_gdrive import upload_to_drive

# === CONFIG ===
PARTNERS = [
    {
        "id": "1384",
        "name": "Commonwealth Race Management",
        "url": "https://runsignup.com/Partner/Participants/Report/1384"
    },
    {
        "id": "1385",
        "name": "PR Races",
        "url": "https://runsignup.com/Partner/Participants/Report/1385"
    },
    {
        "id": "1411",
        "name": "PR Training Programs",
        "url": "https://runsignup.com/Partner/Participants/Report/1411"
    }
]

DOWNLOAD_DIR = os.path.join(os.getcwd(), "optimizely_connector", "output")
COOKIE_PATH = os.path.join(DOWNLOAD_DIR, "runsignup_cookies.json")
DEBUG_SCREENSHOT = os.path.join(DOWNLOAD_DIR, "debug_screen.png")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })

    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": DOWNLOAD_DIR
    })
    return driver

def load_cookies(driver):
    if not os.path.exists(COOKIE_PATH):
        raise FileNotFoundError("❌ Cookie file not found. Run save_cookies.py first.")
    driver.get("https://runsignup.com")
    with open(COOKIE_PATH, "r") as f:
        cookies = json.load(f)
        for cookie in cookies:
            driver.add_cookie(cookie)

def wait_for_and_download(driver, partner_url, partner_id):
    driver.get(partner_url)

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//table"))
        )
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(text(), 'Export Options')]"))
        )
        export_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export Options')]"))
        )
        export_button.click()

        download_link = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Download Report As CSV')]"))
        )
        download_link.click()
    except Exception as e:
        print(f"❌ Failed for partner {partner_id}: {e}")
        raise

    for _ in range(30):
        files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
        if files:
            latest = max(files, key=lambda f: os.path.getctime(os.path.join(DOWNLOAD_DIR, f)))
            timestamp = datetime.now().strftime("%Y-%m-%d")
            new_name = f"runsignup_export_{partner_id}_{timestamp}.csv"
            new_path = os.path.join(DOWNLOAD_DIR, new_name)
            os.rename(os.path.join(DOWNLOAD_DIR, latest), new_path)
            print(f"✅ Downloaded: {new_name}")
            return new_path
        time.sleep(1)

    raise FileNotFoundError(f"❌ Timed out waiting for CSV for partner {partner_id}")

def main():
    print("🚀 Starting full RunSignUp CSV download for all partners...")
    driver = setup_driver()

    try:
        load_cookies(driver)

        for partner in PARTNERS:
            print(f"\n=== Downloading for Partner ID {partner['id']} ({partner['name']}) ===")
            try:
                csv_path = wait_for_and_download(driver, partner["url"], partner["id"])
                upload_to_drive(csv_path)
                print("📤 Upload complete.")
            except Exception as e:
                print(f"❌ Skipped {partner['id']} due to error: {e}")
    finally:
        driver.quit()
        print("🧼 Browser closed.")

if __name__ == "__main__":
    main()
