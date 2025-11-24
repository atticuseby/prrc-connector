import os, sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from datetime import datetime, timedelta
import requests
import json
import argparse
from rics_connector.fetch_rics_data import fetch_rics_data_with_purchase_history
from scripts.helpers import log_message

def main():
    parser = argparse.ArgumentParser(description="RICS Live Sync with Debug Counters")
    parser.add_argument('--no-dedup', action='store_true', help='Skip deduplication step')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    args = parser.parse_args()
    
    log_message("=== RICS LIVE SYNC WITH DEBUG COUNTERS ===")
    log_message(f"🔧 Debug mode: {args.debug}")
    log_message(f"🔧 No-dedup mode: {args.no_dedup}")
    
    # Initialize counters
    counters = {
        'raw_count': 0,
        'after_cutoff_count': 0,
        'after_dedup_count': 0,
        'api_errors': 0,
        'empty_responses': 0
    }
    
    # Test RICS API connection first
    log_message("=== RICS API CONNECTION TEST ===")
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        log_message("❌ Missing RICS_API_TOKEN")
        return 1
    log_message(f"✅ Token present, length={len(token)}")
    
    # Test API endpoint
    start_date = (datetime.utcnow() - timedelta(days=45)).strftime("%Y-%m-%dT%H:%M:%SZ")  # 45 days to capture September 30th data
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    test_payload = {
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
        log_message(f"🔍 Testing API endpoint: {url}")
        log_message(f"🔍 Date range: {start_date} to {end_date}")
        resp = requests.post(url, headers={"Token": token}, json=test_payload, timeout=30)
        log_message(f"📊 API Response: {resp.status_code}")
        
        if resp.status_code == 401:
            log_message("❌ 401 Unauthorized - RICS API token is invalid or expired!")
            log_message("🔧 ACTION REQUIRED: Update RICS_API_TOKEN secret in GitHub")
            return 1
        elif resp.status_code != 200:
            log_message(f"❌ API Error {resp.status_code}: {resp.text[:200]}")
            return 1
            
        data = resp.json()
        sales = data.get("Sales", [])
        log_message(f"✅ API Test successful - returned {len(sales)} sales")
        
        if sales:
            log_message(f"🔍 Sample sale structure: {list(sales[0].keys())}")
            if 'SaleHeaders' in sales[0]:
                log_message(f"🔍 SaleHeaders count: {len(sales[0]['SaleHeaders'])}")
        
    except Exception as e:
        log_message(f"❌ API test failed: {e}")
        return 1
    
    # Run full RICS fetch with counters
    log_message("=== RUNNING FULL RICS FETCH ===")
    try:
        if args.no_dedup:
            log_message("🔧 Skipping deduplication as requested")
            # We'll modify the fetch function to skip dedup
            output_path = fetch_rics_data_with_purchase_history(debug_mode=args.debug, no_dedup=True)
        else:
            output_path = fetch_rics_data_with_purchase_history(debug_mode=args.debug)
        
        log_message(f"✅ RICS fetch completed: {output_path}")
        
        # Analyze the output file
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            log_message(f"📊 Output file size: {file_size} bytes")
            
            with open(output_path, 'r') as f:
                lines = f.readlines()
                counters['raw_count'] = len(lines) - 1  # Subtract header
                log_message(f"📊 Total rows in CSV: {counters['raw_count']}")
                
                if counters['raw_count'] == 0:
                    log_message("⚠️ WARNING: No data rows found!")
                    log_message("🔍 Possible causes:")
                    log_message("  - RICS API token expired")
                    log_message("  - No transactions in last 7 days")
                    log_message("  - Date filter too restrictive")
                    log_message("  - API endpoint changed")
                else:
                    log_message(f"✅ Found {counters['raw_count']} data rows")
                    # Show sample data
                    if len(lines) > 1:
                        log_message(f"🔍 Sample row: {lines[1].strip()[:100]}...")
        else:
            log_message("❌ ERROR: Output file not created!")
            return 1
            
    except Exception as e:
        log_message(f"❌ Error during RICS fetch: {e}")
        import traceback
        log_message(f"Traceback: {traceback.format_exc()}")
        return 1
    
    # Create deduplicated version if not skipping
    if not args.no_dedup:
        log_message("=== CREATING DEDUPLICATED VERSION ===")
        import shutil
        base_name = os.path.basename(output_path)
        deduped_filename = base_name.replace('.csv', '_deduped.csv')
        deduped_path = os.path.join(os.path.dirname(output_path), deduped_filename)
        
        try:
            shutil.copy2(output_path, deduped_path)
            log_message(f"✅ Created deduplicated file: {deduped_path}")
            
            # Count rows in deduped file
            with open(deduped_path, 'r') as f:
                deduped_lines = f.readlines()
                counters['after_dedup_count'] = len(deduped_lines) - 1
                log_message(f"📊 Rows after dedup: {counters['after_dedup_count']}")
            
            # Create static symlink for downstream processes
            static_deduped_path = "rics_customer_purchase_history_deduped.csv"
            if os.path.exists(static_deduped_path):
                os.remove(static_deduped_path)
            os.symlink(deduped_path, static_deduped_path)
            log_message(f"✅ Created static symlink: {static_deduped_path}")
            
        except Exception as e:
            log_message(f"❌ Error creating deduplicated file: {e}")
            return 1
    else:
        log_message("🔧 Skipping deduplication - using raw file")
        # Create symlink to raw file
        static_deduped_path = "rics_customer_purchase_history_deduped.csv"
        if os.path.exists(static_deduped_path):
            os.remove(static_deduped_path)
        os.symlink(output_path, static_deduped_path)
        log_message(f"✅ Created symlink to raw file: {static_deduped_path}")
    
    # Upload to Google Drive (optional)
    log_message("=== UPLOADING TO GOOGLE DRIVE ===")
    try:
        from scripts.upload_to_gdrive import upload_to_drive
        upload_to_drive(output_path)
        if not args.no_dedup:
            upload_to_drive(deduped_path)
        log_message("✅ Successfully uploaded files to Google Drive")
    except Exception as e:
        log_message(f"⚠️ Google Drive upload skipped: {e}")
        log_message("ℹ️ Files are available locally for testing")
    
    # Log final counters
    log_message("=== FINAL COUNTERS ===")
    log_message(f"📊 Raw rows fetched: {counters['raw_count']}")
    log_message(f"📊 After cutoff filter: {counters['after_cutoff_count']}")
    log_message(f"📊 After deduplication: {counters['after_dedup_count']}")
    log_message(f"📊 API errors: {counters['api_errors']}")
    log_message(f"📊 Empty responses: {counters['empty_responses']}")
    
    # Save counters to file for analysis
    counters_file = "logs/sync_counters.json"
    os.makedirs("logs", exist_ok=True)
    with open(counters_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'counters': counters,
            'no_dedup': args.no_dedup,
            'debug_mode': args.debug
        }, f, indent=2)
    log_message(f"📊 Counters saved to: {counters_file}")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)