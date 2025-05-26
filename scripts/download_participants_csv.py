import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
from scripts.upload_to_gdrive import upload_to_drive

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(message)s")

# Constants
TARGET_URL = "https://runsignup.com/Partner/Participants/Report/1385"
DOWNLOAD_DIR = os.path.abspath("output")
EXPORT_FILENAME = f"runsignup_export_{datetime.now().strftime('%Y-%m-%d')}.csv"
RUNSIGNUP_FULL_COOKIE_HEADER = os.getenv("RUNSIGNUP_FULL_COOKIE_HEADER")

if not RUNSIGNUP_FULL_COOKIE_HEADER:
    logging.error("❌ RUNSIGNUP_FULL_COOKIE_HEADER not set")
    exit(1)

# Set up headless Chrome
options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920x1080")
options.add_argument("--no-sandbox")
prefs = {"download.default_directory": DOWNLOAD_DIR}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 20)

try:
    logging.info("🌐 Opening page...")
    driver.get("https://runsignup.com")
    driver.delete_all_cookies()

    logging.info("🍪 Injecting session cookies...")
    for pair in RUNSIGNUP_FULL_COOKIE_HEADER.split(";"):
        if "=" not in pair:
            continue
        name, value = pair.strip().split("=", 1)
        driver.add_cookie({"name": name, "value": value, "domain": ".runsignup.com", "path": "/"})

    driver.get(TARGET_URL)
    wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
    logging.info("✅ Page loaded with session cookies.")

    logging.info("📥 Clicking Export CSV button...")
    export_button = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Export to CSV")))
    export_button.click()

    logging.info("⏳ Waiting for download to complete...")
    time.sleep(10)

    downloaded_path = os.path.join(DOWNLOAD_DIR, "PartnerParticipants.csv")
    if not os.path.exists(downloaded_path):
        raise FileNotFoundError("CSV not downloaded")

    final_path = os.path.join(DOWNLOAD_DIR, EXPORT_FILENAME)
    os.rename(downloaded_path, final_path)
    logging.info(f"✅ File renamed to {EXPORT_FILENAME}")

    if os.getenv("UPLOAD_TO_GDRIVE", "true").lower() == "true":
        logging.info("🚀 Uploading to Google Drive...")
        upload_to_drive(final_path, "runsignup")

except Exception as e:
    logging.error(f"❌ Script failed: {e}")

finally:
    driver.quit()
