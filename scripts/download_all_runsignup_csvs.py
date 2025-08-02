import os
import sys
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
        print("⚠️ Cookie file not found. Attempting to regenerate cookies...")
        os.system("python scripts/save_cookies.py")

    try:
        driver.get("https://runsignup.com")
        with open(COOKIE_PATH, "r") as f:
            cookies = json.load(f)
        for cookie in cookies:
            driver.add_cookie(cookie)
        print("✅ Cookies loaded.")
    except Exception as e:
        print(f"❌ Failed to load cookies: {e}")
        raise

def wait_for_and_download(driver, partner_url, partner_id):
    print(f"🌐 Navigating to: {partner_url}")
    driver.get(partner_url)

    try:
        print("⏳ Waiting for table to load...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//table"))
        )
        print("✅ Table found.")

        print("⏳ Waiting for Export Options button...")
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//button[contains(text(), 'Export Options')]"))
        )
        print("✅ Export Options button present.")

        export_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export Options')]"))
        )
        print("🖱️ Clicking Export Options...")
        export_button.click()

        print("⏳ Waiting for 'Download Report As CSV' link...")
        download_link = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Download Report As CSV')]"))
        )
        print("🖱️ Clicking CSV download link...")
        download_link.click()

    except Exception as e:
        print(f"❌ Error in wait_for_and_download for partner {partner_id}: {e}")
        screenshot_path = os.path.join(DOWNLOAD_DIR, f"debug_{partner_id}.png")
        driver.save_screenshot(screenshot_path)
        print(f"📸 Screenshot saved to {screenshot_path}")
        raise

    print("⏳ Waiting for file download...")
    for _ in range(30):
        files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
        if files:
            latest = max(files, key=lambda f: os.path.getctime(os.path.join(DOWNLOAD_DIR, f)))
            timestamp = datetime.now().strftime("%Y-%m-%d")
            new_name = f"runsignup_export_{partner_id}_{timestamp}.csv"
            new_path = os.path.join(DOWNLOAD_DIR, new_name)
            os.rename(os.path.join(DOWNLOAD_DIR, latest), new_path)
            print(f"✅ File downloaded and renamed: {new_name}")
            return new_path
        time.sleep(1)

    raise FileNotFoundError(f"❌ Timed out waiting for CSV download for partner {partner_id}")

def main():
    print("🚀 Starting full RunSignUp CSV download for all partners...")
    driver = setup_driver()

    try:
        try:
            load_cookies(driver)
        except:
            print("🔁 Retrying cookie regeneration...")
            os.system("python scripts/save_cookies.py")
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
