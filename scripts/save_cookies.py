import os
import json
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

EMAIL = os.environ.get("RUNSIGNUP_EMAIL")
PASSWORD = os.environ.get("RUNSIGNUP_PASSWORD")

if not EMAIL or not PASSWORD:
    raise EnvironmentError("❌ RUNSIGNUP_EMAIL or RUNSIGNUP_PASSWORD not set in environment.")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "optimizely_connector", "output")
COOKIE_PATH = os.path.join(DOWNLOAD_DIR, "runsignup_cookies.json")
LOGIN_URL = "https://runsignup.com/Login"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def setup_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

def save_cookies():
    driver = setup_driver()
    try:
        driver.get(LOGIN_URL)

        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.NAME, "email")))
        driver.find_element(By.NAME, "email").send_keys(EMAIL)
        driver.find_element(By.NAME, "password").send_keys(PASSWORD)

        # Wait for the login button and click it via JS to avoid interception
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.XPATH, "//button[@type='submit']")))
        submit_button = driver.find_element(By.XPATH, "//button[@type='submit']")
        driver.execute_script("arguments[0].scrollIntoView(true);", submit_button)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", submit_button)

        # Wait for successful login
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.ID, "mainContent")))
        print("✅ Login successful")

        # Save cookies
        cookies = driver.get_cookies()
        with open(COOKIE_PATH, "w") as f:
            json.dump(cookies, f)
        print(f"🍪 Cookies saved to: {COOKIE_PATH}")

    except Exception as e:
        debug_path = os.path.join(DOWNLOAD_DIR, "login_debug.png")
        driver.save_screenshot(debug_path)
        print(f"📸 Screenshot saved to {debug_path}")
        print(f"❌ Failed to save cookies: {e}")
        raise
    finally:
        driver.quit()

if __name__ == "__main__":
    save_cookies()
