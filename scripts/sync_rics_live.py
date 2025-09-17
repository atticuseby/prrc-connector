import os, sys, ssl, requests, urllib3, traceback
from datetime import datetime, timedelta

# --- CONFIG ---
STORE_CODES = ["1", "2"]  # keep small for debugging; expand once confirmed
DAYS_BACK = 7
PAGE_SIZE = 10  # small page to test
TIMEOUT = 20

# Candidate endpoints (RICS can be picky about casing and "api" prefix)
ENDPOINTS = [
    "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction",
    "https://enterprise.ricssoftware.com/pos/GetPOSTransaction",
    "https://enterprise.ricssoftware.com/POS/GetPOSTransaction",
]

# Candidate header styles
def make_headers(token):
    return [
        {"Token": token},
        {"Authorization": f"Bearer {token}"},
        {"token": token},  # lowercase just in case
    ]

# --- Helpers ---
def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {msg}", flush=True)

def run_debug():
    log("=== RICS TLS / API Debug ===")
    log(f"Python: {sys.version}")
    log(f"OpenSSL: {ssl.OPENSSL_VERSION}")
    log(f"urllib3: {urllib3.__version__}")

    token = os.getenv("RICS_API_TOKEN", "").strip()
    if not token:
        log("❌ Missing RICS_API_TOKEN")
        sys.exit(1)
    else:
        log(f"✅ Token present, length={len(token)}")

    start = (datetime.utcnow() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    for endpoint in ENDPOINTS:
        for header in make_headers(token):
            log(f"\n🔎 Testing endpoint={endpoint} headers={list(header.keys())[0]}")

            payload = {
                "Take": PAGE_SIZE,
                "Skip": 0,
                "TicketDateStart": start,
                "TicketDateEnd": end,
                "StoreCode": STORE_CODES[0],  # just test first store
            }

            try:
                resp = requests.post(endpoint, headers=header, json=payload, timeout=TIMEOUT)
                log(f"→ Status: {resp.status_code}")
                if resp.status_code == 200:
                    log(f"✅ Success! Body (first 300 chars): {resp.text[:300]}")
                    return  # stop early once something works
                else:
                    log(f"Body: {resp.text[:200]}")
            except Exception as e:
                log(f"❌ Exception: {repr(e)}")
                traceback.print_exc()

    log("⚠️ Tried all combos, none succeeded. Check TLS or endpoint settings.")

if __name__ == "__main__":
    run_debug()
