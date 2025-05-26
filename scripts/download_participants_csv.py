from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import os

# Setup
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 20)

try:
    print("🌐 Opening page...")
    driver.get("https://runsignup.com/Partner/Participants/Report/1385")

    print("🍪 Injecting session cookie...")
    cookie_value = os.getenv("RUNSIGNUP_SESSION_COOKIE")
    if not cookie_value:
        raise Exception("RUNSIGNUP_SESSION_COOKIE not set")

    driver.add_cookie({
        'name': 'RSUSession',
        'value': cookie_value,
        'domain': 'runsignup.com',
        'path': '/'
    })

    # Refresh to apply cookie
    driver.get("https://runsignup.com/Partner/Participants/Report/1385")
    time.sleep(3)

    print("📥 Downloading CSV...")
    export_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "Export CSV")]')))
    export_button.click()

    print("✅ Export initiated (check downloads or logs)")
except Exception as e:
    print(f"❌ Script failed: {e}")
finally:
    driver.quit()
