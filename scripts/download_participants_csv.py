import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

try:
    print("🌐 Opening page...")

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    driver.get("https://runsignup.com/Partner/Participants/Report/1385")

    print("🍪 Injecting session cookies...")
    cookie_header = os.getenv("RUNSIGNUP_FULL_COOKIE_HEADER")
    if not cookie_header:
        raise ValueError("RUNSIGNUP_FULL_COOKIE_HEADER not set")

    # Parse cookie header and add cookies to Selenium session
    for pair in cookie_header.split(";"):
        if "=" in pair:
            name, value = pair.strip().split("=", 1)
            driver.add_cookie({"name": name, "value": value})

    # Reload page after cookies are injected
    driver.get("https://runsignup.com/Partner/Participants/Report/1385")
    time.sleep(5)

    print("✅ Page loaded with session cookies. (Placeholder for download logic)")
    driver.quit()

except Exception as e:
    print(f"❌ Script failed: {e}")
    driver.quit()
    raise e
