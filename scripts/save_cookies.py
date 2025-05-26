# scripts/save_cookies.py

import os
import time
import json
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

COOKIE_PATH = os.path.join("optimizely_connector", "output", "runsignup_cookies.json")
LOGIN_URL = "https://runsignup.com/Login"

os.makedirs(os.path.dirname(COOKIE_PATH), exist_ok=True)

def save_cookies():
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Headful mode so you can login manually
    driver = webdriver.Chrome(options=chrome_options)

    driver.get(LOGIN_URL)
    print("🔓 Please log in manually...")
    input("✅ Press ENTER after login is complete: ")

    cookies = driver.get_cookies()
    with open(COOKIE_PATH, "w") as f:
        json.dump(cookies, f)

    print(f"🍪 Cookies saved to {COOKIE_PATH}")
    driver.quit()

if __name__ == "__main__":
    save_cookies()
