# scripts/download_participants_csv.py

import os
import time
import json
import base64
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from upload_to_gdrive import upload_to_drive

DOWNLOAD_DIR = os.path.join(os.getcwd(), "optimizely_connector", "output")
COOKIE_PATH = os.path.join(DOWNLOAD_DIR, "runsignup_cookies.json")
PARTICIPANT_URL = "https://runsignup.com/Partner/Participants/Report/1385"
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

    print("🍪 Loading saved cookies...")
    driver.get("https://runsignup.com")
    with open(COOKIE_PATH, "r") as f:
        cookies = json.load(f)
        for cookie in cookies:
            driver.add_cookie(cookie)

    driver.get(PARTICIPANT_URL)

def download_csv(driver):
    print("📸 Capturing screenshot for debug...")
    driver.save_screenshot(DEBUG_SCREENSHOT)

    print("📥 Waiting for Export Options button...")
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
        print("✅ Export button found. Clicking now...")
        export_button.click()

        print("📥 Waiting for 'Download Report As CSV' link...")
        download_link = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Download Report As CSV')]"))
        )
        print("✅ CSV link found. Clicking now...")
        download_link.click()
    except Exception as e:
        print("❌ Exception encountered in download_csv")
        print(f"Exception: {e}")
        raise TimeoutError("❌ Could not find or click Export dropdown or CSV link.") from e

def wait_for_download():
    print("⏳ Waiting for file download...")
    for _ in range(30):
        files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
        if files:
            latest = max(files, key=lambda f: os.path.getctime(os.path.join(DOWNLOAD_DIR, f)))
            timestamp = datetime.now().strftime("%Y-%m-%d")
            final_filename = f"run_signup_export_{timestamp}.csv"
            final_path = os.path.join(DOWNLOAD_DIR, final_filename)
            os.rename(os.path.join(DOWNLOAD_DIR, latest), final_path)
            print(f"✅ File saved as: {final_path}")
            return final_path
        time.sleep(1)
    raise FileNotFoundError("❌ Timed out waiting for CSV download.")

def dump_debug_image():
    if os.path.exists(DEBUG_SCREENSHOT):
        with open(DEBUG_SCREENSHOT, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
            print(f"\n--- DEBUG SCREENSHOT BASE64 ---\n{b64}\n--- END DEBUG SCREENSHOT ---\n")

def main():
    print("🧪 Script is running...")
    print("🚀 Starting RunSignUp automation...")
    driver = setup_driver()
    try:
        load_cookies(driver)
        download_csv(driver)
        csv_path = wait_for_download()
        upload_to_drive(csv_path)
        print("📤 Upload to Drive complete.")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        dump_debug_image()
        raise
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
