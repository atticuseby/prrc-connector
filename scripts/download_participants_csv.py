# scripts/download_participants_csv.py

import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# 🔐 Environment variables
RUNSIGNUP_EMAIL = os.getenv("RUNSIGNUP_EMAIL")
RUNSIGNUP_PASSWORD = os.getenv("RUNSIGNUP_PASSWORD")
DOWNLOAD_URL = "https://runsignup.com/Partner/ParticipantsReport/1385"

# 🧱 Chrome setup
options = Options()
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
options.add_argument('--headless=new')  # Use old '--headless' if errors
prefs = {"download.default_directory": os.getcwd()}
options.add_experimental_option("prefs", prefs)

driver = webdriver.Chrome(options=options)

try:
    # 🌐 Open login page
    print("🌐 Navigating to login page...")
    driver.get("https://runsignup.com/Login")

    # 🧍 Log in
    print("🔐 Logging in...")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "email"))).send_keys(RUNSIGNUP_EMAIL)
    driver.find_element(By.NAME, "password").send_keys(RUNSIGNUP_PASSWORD + Keys.RETURN)

    # ✅ Wait for login redirect
    WebDriverWait(driver, 10).until(EC.url_contains("runsignup.com/Profile"))
    print("✅ Logged in successfully")

    # 📄 Go to participants report
    print("📄 Navigating to participants report page...")
    driver.get(DOWNLOAD_URL)

    # 🕐 Wait for export button to appear
    export_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.LINK_TEXT, "Export All Columns"))
    )

    # 💾 Click export
    print("💾 Clicking export button...")
    export_button.click()

    # ⏳ Wait for download
    print("⏳ Waiting for download to complete...")
    time.sleep(10)

    print("✅ CSV download complete (check your output folder)")

except Exception as e:
    print(f"❌ Script failed: {e}")

finally:
    driver.quit()
