import os
import requests
import traceback
from datetime import datetime, timedelta

ENDPOINT = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
TOKEN = os.getenv("RICS_API_TOKEN", "").strip()

def log(msg):
    print(f"{datetime.utcnow().isoformat()} | {msg}", flush=True)

def validate():
    if not TOKEN:
        log("❌ Missing RICS_API_TOKEN in environment.")
        return

    headers = {"Token": TOKEN}
    start = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = {
        "Take": 1,
        "Skip": 0,
        "TicketDateStart": start,
        "TicketDateEnd": end,
        "StoreCode": "1"  # small test request
    }

    try:
        log(f"📤 Sending validation request → {ENDPOINT}")
        resp = requests.post(ENDPOINT, headers=headers, json=payload, timeout=15)

        log(f"Status = {resp.status_code}")
        log(f"Body   = {resp.text[:400]}...")

        if resp.status_code == 200:
            log("✅ Token is valid and POS-enabled.")
        elif resp.status_code == 401:
            log("❌ Unauthorized → Token exists but is invalid or not POS-enabled.")
        elif resp.status_code == 429 or "Request Limit Reached" in resp.text:
            log("⚠️ Token is valid but API quota exceeded. Wait for reset or request higher limit.")
        elif resp.status_code == 404:
            log("❌ Endpoint not found. Double-check that you’re hitting the POS API.")
        else:
            log("❌ Unexpected error. Check body above for details.")

    except Exception as e:
        log(f"❌ Exception: {repr(e)}")
        traceback.print_exc()

if __name__ == "__main__":
    validate()
