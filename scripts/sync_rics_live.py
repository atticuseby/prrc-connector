import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta
import requests
import json
from rics_connector.fetch_rics_data import fetch_rics_data_with_purchase_history
from scripts.helpers import log_message

if __name__ == "__main__":
    log_message("=== RICS TLS / API Debug ===")

    import ssl, urllib3
    log_message(f"Python: {sys.version}")
    log_message(f"OpenSSL: {ssl.OPENSSL_VERSION}")
    log_message(f"urllib3: {urllib3.__version__}")

    token = os.getenv("RICS_API_TOKEN")
    if not token:
        raise RuntimeError("❌ Missing RICS_API_TOKEN")
    log_message(f"✅ Token present, length={len(token)}")

    # Dates - Extended to 1 year to find ANY data
    start_date = (datetime.utcnow() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    payload = {
        "Take": 1,
        "Skip": 0,
        "TicketDateStart": start_date,
        "TicketDateEnd": end_date,
        "BatchStartDate": start_date,
        "BatchEndDate": end_date,
        "StoreCode": "1"
    }

    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    try:
        log_message(f"↪ Testing endpoint={url} payload={payload}")
        resp = requests.post(url, headers={"Token": token}, json=payload, timeout=30)
        log_message(f"→ Status: {resp.status_code}")
        log_message(f"✅ Success! Body (first 300 chars): {resp.text[:300]}")
    except Exception as e:
        log_message(f"❌ Validation call failed: {e}")

    log_message("=== Running full RICS fetch ===")
    try:
        output_path = fetch_rics_data_with_purchase_history()
        log_message(f"✅ Completed full fetch, file at {output_path}")
        
        # Check if file has actual data (more than just headers)
        import os
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            log_message(f"📊 Output file size: {file_size} bytes")
            
            # Count lines in the file
            with open(output_path, 'r') as f:
                lines = f.readlines()
                log_message(f"📊 Number of lines in file: {len(lines)}")
                
                if len(lines) <= 1:
                    log_message("⚠️ WARNING: File contains only headers or is empty!")
                    log_message("🔍 This suggests no data was fetched from RICS API")
                else:
                    log_message(f"✅ File contains {len(lines)-1} data rows (plus header)")
        else:
            log_message("❌ ERROR: Output file was not created!")
            raise RuntimeError("Output file not created")
            
    except Exception as e:
        log_message(f"❌ Error during RICS fetch: {e}")
        import traceback
        log_message(f"Traceback: {traceback.format_exc()}")
        raise
    
    # Create deduplicated version for downstream processes
    import shutil
    deduped_path = "rics_customer_purchase_history_deduped.csv"
    try:
        shutil.copy2(output_path, deduped_path)
        log_message(f"✅ Created deduplicated file: {deduped_path}")
    except Exception as e:
        log_message(f"❌ Error creating deduplicated file: {e}")
        raise
    
    # Upload to Google Drive
    log_message("=== Uploading to Google Drive ===")
    try:
        from scripts.upload_to_gdrive import upload_to_drive
        upload_to_drive(output_path)
        upload_to_drive(deduped_path)
        log_message("✅ Successfully uploaded both files to Google Drive")
    except Exception as e:
        log_message(f"❌ Google Drive upload failed: {e}")
        raise
