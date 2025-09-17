import os
import sys
from datetime import datetime, timedelta
from scripts.fetch_rics_data import fetch_rics_data_with_purchase_history
from scripts.helpers import log_message
import requests
import json

if __name__ == "__main__":
    log_message("=== RICS TLS / API Debug ===")

    # Debug TLS environment
    import ssl, urllib3
    log_message(f"Python: {sys.version}")
    log_message(f"OpenSSL: {ssl.OPENSSL_VERSION}")
    log_message(f"urllib3: {urllib3.__version__}")

    # --- Token presence ---
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        raise RuntimeError("❌ Missing RICS_API_TOKEN")
    log_message(f"✅ Token present, length={len(token)}")

    # --- Validation ping with BatchStart/BatchEnd ---
    start_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "Take": 1,
        "Skip": 0,
        "TicketDateStart": start_date,
        "TicketDateEnd": end_date,
        "BatchStartDate": start_date,
        "BatchEndDate": end_date,
        "StoreCode": "1"  # use one known store for validation
    }

    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    try:
        log_message(f"↪ Testing endpoint={url} headers=Token payload={payload}")
        resp = requests.post(url, headers={"Token": token}, json=payload, timeout=30)
        log_message(f"→ Status: {resp.status_code}")
        log_message(f"✅ Success! Body (first 300 chars): {resp.text[:300]}")
    except Exception as e:
        log_message(f"❌ Validation call failed: {e}")

    # --- Run full fetch ---
    log_message("=== Running full RICS fetch ===")
    output_path = fetch_rics_data_with_purchase_history()
    log_message(f"✅ Completed full fetch, file at {output_path}")
