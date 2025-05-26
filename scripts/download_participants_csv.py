import os
import time
import base64
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from upload_to_gdrive import upload_to_drive

DOWNLOAD_DIR = os.path.join(os.getcwd(), "optimizely_connector", "output")
FILENAME = "run_signup_export.csv"
TARGET_URL = "https://runsignup.com/Partner/Participants/Report/1385"
DEBUG_SCREENSHOT = os.path.join(DOWNLOAD_DIR, "debug_screen.png")

# Ensure output folder exists
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
    driver.execute_cdp_cmd("Network.enable", {})
    return driver

def inject_cookies(driver):
    cookie_header = os.environ.get("RUNSIGNUP_FULL_COOKIE_HEADER")
    if not cookie_header:
        raise ValueError("❌ Missing RUNSIGNUP_FULL_COOKIE_HEADER")

    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
        "headers": {
            "Cookie": cookie_header,
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
        }
    })

    driver.get(TARGET_URL)

def download_csv(driver):
    print("📸 Capturing screenshot for debug...")
    driver.save_screenshot(DEBUG_SCREENSHOT)

    print("🔍 Waiting for 'Export CSV' button...")
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.LINK_TEXT, "Export CSV"))
    )

    export_button = driver.find_element(By.LINK_TEXT, "Export CSV")
    ActionChains(driver).move_to_element(export_button).click().perform()
    print("📥 Export clicked")

def wait_for_download():
    print("⏳ Waiting for CSV download...")
    for _ in range(30):
        files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
        if files:
            latest = max(files, key=lambda f: os.path.getctime(os.path.join(DOWNLOAD_DIR, f)))
            final_path = os.path.join(DOWNLOAD_DIR, FILENAME)
            os.rename(os.path.join(DOWNLOAD_DIR, latest), final_path)
            print(f"✅ File downloaded: {final_path}")
            return final_path
        time.sleep(1)
    raise FileNotFoundError("❌ Timed out waiting for CSV file.")

def dump_debug_image():
    if os.path.exists(DEBUG_SCREENSHOT):
        with open(DEBUG_SCREENSHOT, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
            print(f"\n--- DEBUG SCREENSHOT BASE64 ---\n{b64}\n--- END DEBUG SCREENSHOT ---\n")

def main():
    print("🚀 Starting RunSignUp download automation...")
    driver = setup_driver()
    try:
        inject_cookies(driver)
        download_csv(driver)
        csv_path = wait_for_download()
        upload_to_drive(csv_path)
        print("📤 Upload complete")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        dump_debug_image()
        raise
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
