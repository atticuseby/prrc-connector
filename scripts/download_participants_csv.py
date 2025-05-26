import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

EMAIL = os.getenv("RUNSIGNUP_EMAIL")
PASSWORD = os.getenv("RUNSIGNUP_PASSWORD")

options = Options()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(options=options)

try:
    print("🌐 Navigating to login page...")
    driver.get("https://runsignup.com/Login")

    print("🔐 Logging in...")
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "email")))
    driver.find_element(By.NAME, "email").send_keys(EMAIL)
    driver.find_element(By.NAME, "password").send_keys(PASSWORD)

    login_button = driver.find_element(By.XPATH, "//button[@type='submit']")

    # Scroll button into view and wait for it to be clickable
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", login_button)
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit']")))

    time.sleep(1)  # Let overlays finish animating
    login_button.click()

    time.sleep(3)
    if "Dashboard" not in driver.page_source and "My Profile" not in driver.page_source:
        raise Exception("Login failed — check credentials or 2FA prompts.")

    print("✅ Login successful!")

    # Go to participant export
    url = "https://runsignup.com/Partner/Participants/Report/1385"
    print(f"🌐 Navigating to {url}")
    driver.get(url)
    time.sleep(3)

    print("⬇️ Clicking Export to CSV...")
    export_button = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, "//input[@type='submit' and contains(@value, 'Export to CSV')]"))
    )
    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", export_button)
    export_button.click()

    print("✅ CSV export triggered!")

except Exception as e:
    print(f"❌ Script failed: {e}")

finally:
    driver.quit()
