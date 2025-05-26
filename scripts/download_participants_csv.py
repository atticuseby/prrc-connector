import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from scripts.upload_to_gdrive import upload_to_drive

# ENV setup
FULL_COOKIE_HEADER = os.getenv("RUNSIGNUP_FULL_COOKIE_HEADER", "").strip()
DOWNLOAD_DIR = os.path.abspath("optimizely_connector/output")

if not FULL_COOKIE_HEADER:
    raise Exception("❌ RUNSIGNUP_FULL_COOKIE_HEADER not set in environment variables.")

# Chrome config
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

print("🌐 Opening page...")
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://runsignup.com/Partner/Participants/Report/1385")

print("🍪 Injecting session cookies...")
for cookie_str in FULL_COOKIE_HEADER.split(";"):
    if "=" in cookie_str:
        name, value = cookie_str.strip().split("=", 1)
        driver.add_cookie({"name": name.strip(), "value": value.strip(), "domain": ".runsignup.com"})

driver.refresh()

try:
    print("📄 Waiting for download button...")
    download_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "form[action*='Download'] button[type='submit']"))
    )

    print("⬇️ Clicking download...")
    ActionChains(driver).move_to_element(download_button).click().perform()

    print("⏳ Waiting for file to finish downloading...")
    timeout = time.time() + 15
    downloaded_file = None

    while time.time() < timeout:
        for file in os.listdir(DOWNLOAD_DIR):
            if file.startswith("ParticipantReport") and file.endswith(".csv"):
                downloaded_file = os.path.join(DOWNLOAD_DIR, file)
                break
        if downloaded_file:
            break
        time.sleep(1)

    if not downloaded_file:
        raise Exception("❌ Download failed — file not found.")

    print(f"✅ File downloaded: {downloaded_file}")
    upload_to_drive(downloaded_file)

finally:
    driver.quit()
