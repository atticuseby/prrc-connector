import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from upload_to_gdrive import upload_to_drive  # ✅ Local import

DOWNLOAD_DIR = os.path.join(os.getcwd(), "optimizely_connector", "output")
FILENAME = "run_signup_export.csv"

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
    return driver

def inject_cookies(driver):
    cookie_header = os.environ.get("RUNSIGNUP_FULL_COOKIE_HEADER")
    if not cookie_header:
        raise ValueError("Missing RUNSIGNUP_FULL_COOKIE_HEADER env variable")
    
    driver.get("https://runsignup.com/Partner/Participants/Report/1385")
    driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {
        "headers": {
            "Cookie": cookie_header
        }
    })
    driver.refresh()

def download_csv(driver):
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.LINK_TEXT, "Export CSV")))
    export_button = driver.find_element(By.LINK_TEXT, "Export CSV")
    ActionChains(driver).move_to_element(export_button).click().perform()
    print("📥 CSV download triggered...")

def wait_for_download():
    for _ in range(30):
        files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".csv")]
        if files:
            latest_file = max(files, key=lambda f: os.path.getctime(os.path.join(DOWNLOAD_DIR, f)))
            print(f"✅ Downloaded: {latest_file}")
            new_path = os.path.join(DOWNLOAD_DIR, FILENAME)
            os.rename(os.path.join(DOWNLOAD_DIR, latest_file), new_path)
            return new_path
        time.sleep(1)
    raise FileNotFoundError("❌ Timed out waiting for CSV download.")

def main():
    print("🚀 Starting RunSignUp CSV download flow...")
    driver = setup_driver()
    try:
        inject_cookies(driver)
        download_csv(driver)
        downloaded_file = wait_for_download()
        upload_to_drive(downloaded_file)
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
