import os
import time
import base64
import requests
from datetime import datetime
from urllib.parse import unquote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from upload_to_gdrive import upload_to_drive

DOWNLOAD_DIR = os.path.join(os.getcwd(), "optimizely_connector", "output")
LOGIN_URL = "https://runsignup.com/"
PARTICIPANT_URL = "https://runsignup.com/Partner/Participants/Report/1385"
DEBUG_SCREENSHOT = os.path.join(DOWNLOAD_DIR, "debug_screen.png")

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

    # Allow downloads in headless mode
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": DOWNLOAD_DIR
    })

    return driver

def inject_cookies(driver):
    cookie_header = os.environ.get("RUNSIGNUP_FULL_COOKIE_HEADER")
    if not cookie_header:
        raise ValueError("❌ Missing RUNSIGNUP_FULL_COOKIE_HEADER")

    print("🍪 Injecting cookies...")
    driver.get(LOGIN_URL)

    for pair in cookie_header.split("; "):
        if "=" not in pair:
            continue
        name, value = pair.split("=", 1)
        try:
            driver.add_cookie({
                "name": name,
                "value": unquote(value),
                "domain": "runsignup.com",
                "path": "/"
            })
        except Exception as e:
            print(f"⚠️ Skipping cookie '{name}': {e}")

    driver.get(PARTICIPANT_URL)
    driver.save_screenshot(DEBUG_SCREENSHOT)

def download_csv_via_requests():
    print("📥 Downloading CSV directly via requests...")

    cookie_header = os.environ.get("RUNSIGNUP_FULL_COOKIE_HEADER")
    if not cookie_header:
        raise ValueError("❌ Missing RUNSIGNUP_FULL_COOKIE_HEADER")

    cookies = {}
    for pair in cookie_header.split("; "):
        if "=" in pair:
            k, v = pair.split("=", 1)
            cookies[k] = v

    csv_url = PARTICIPANT_URL.replace("Participants/Report", "Participants/ReportDownloadCSV")
    timestamp = datetime.now().strftime("%Y-%m-%d")
    final_filename = f"run_signup_export_{timestamp}.csv"
    final_path = os.path.join(DOWNLOAD_DIR, final_filename)

    response = requests.get(csv_url, cookies=cookies)

    if "text/csv" not in response.headers.get("Content-Type", ""):
        print("❌ Unexpected response type. Dumping content for inspection:")
        print(response.text[:500])
        raise ValueError("❌ CSV download failed or returned unexpected content.")

    with open(final_path, "wb") as f:
        f.write(response.content)

    print(f"✅ File downloaded and saved as: {final_path}")
    return final_path

def dump_debug_image():
    if os.path.exists(DEBUG_SCREENSHOT):
        with open(DEBUG_SCREENSHOT, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")
            print(f"\n--- DEBUG SCREENSHOT BASE64 ---\n{b64}\n--- END DEBUG SCREENSHOT ---\n")

def main():
    print("🚀 Starting RunSignUp automation...")
    driver = setup_driver()
    try:
        inject_cookies(driver)
        csv_path = download_csv_via_requests()
        upload_to_drive(csv_path)
        print("📤 Upload to Drive complete.")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        dump_debug_image()
        raise
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
