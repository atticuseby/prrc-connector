import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys

# Load credentials from environment
EMAIL = os.getenv("RUNSIGNUP_EMAIL")
PASSWORD = os.getenv("RUNSIGNUP_PASSWORD")

# Headless Chrome setup
options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)

try:
    print("🌐 Navigating to login page...")
    driver.get("https://runsignup.com/Login")

    print("🔐 Logging in...")
    email_input = driver.find_element(By.ID, "user_email")
    password_input = driver.find_element(By.ID, "user_password")
    login_button = driver.find_element(By.NAME, "commit")

    email_input.send_keys(EMAIL)
    password_input.send_keys(PASSWORD)
    login_button.click()

    time.sleep(3)

    if "Dashboard" not in driver.page_source:
        raise Exception("Login failed — credentials might be incorrect or 2FA is enabled.")

    print("✅ Login successful!")

    # Navigate to participant export page
    participants_url = "https://runsignup.com/Partner/Participants/Report/1385"
    print(f"🌐 Navigating to {participants_url}")
    driver.get(participants_url)
    time.sleep(3)

    print("⬇️ Clicking Export to CSV...")
    export_button = driver.find_element(By.XPATH, "//input[@type='submit' and contains(@value, 'Export to CSV')]")
    export_button.click()

    print("✅ CSV export triggered!")

except Exception as e:
    print(f"❌ Script failed: {e}")

finally:
    driver.quit()
