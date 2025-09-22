#!/usr/bin/env python3
"""
Debug script to test the exact same logic as GitHub Actions
"""
import os
import sys
from datetime import datetime, timedelta

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

def test_rics_api():
    """Test the RICS API with the exact same parameters as the workflow"""
    
    # Check if we have the token (this will be False locally, True in GitHub Actions)
    token = os.getenv("RICS_API_TOKEN")
    print(f"RICS_API_TOKEN present: {bool(token)}")
    
    if not token:
        print("❌ No RICS_API_TOKEN found - this is expected locally")
        print("✅ In GitHub Actions, this should be available")
        return
    
    # Test the exact same date range as the workflow
    start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"Date range: {start_date} to {end_date}")
    
    # Test with store code 1 (as integer, not string)
    import requests
    payload = {
        "Take": 10,  # Small number for testing
        "Skip": 0,
        "TicketDateStart": start_date,
        "TicketDateEnd": end_date,
        "BatchStartDate": start_date,
        "BatchEndDate": end_date,
        "StoreCode": 1  # Integer, not string
    }
    
    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    
    try:
        print(f"Testing API call to {url}")
        print(f"Payload: {payload}")
        
        resp = requests.post(
            url, 
            headers={"Token": token}, 
            json=payload, 
            timeout=30
        )
        
        print(f"Status Code: {resp.status_code}")
        print(f"Response Headers: {dict(resp.headers)}")
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"Response keys: {list(data.keys())}")
            
            sales = data.get("Sales", [])
            print(f"Number of sales returned: {len(sales)}")
            
            if sales:
                print(f"First sale keys: {list(sales[0].keys())}")
                print(f"First sale: {sales[0]}")
            else:
                print("No sales data in response")
                
        else:
            print(f"Error response: {resp.text}")
            
    except Exception as e:
        print(f"Exception during API call: {e}")

def test_full_fetch():
    """Test the full fetch process"""
    print("\n" + "="*50)
    print("TESTING FULL FETCH PROCESS")
    print("="*50)
    
    try:
        from rics_connector.fetch_rics_data import fetch_rics_data_with_purchase_history
        
        print("Calling fetch_rics_data_with_purchase_history()...")
        output_path = fetch_rics_data_with_purchase_history()
        print(f"✅ Fetch completed. Output file: {output_path}")
        
        # Check if file exists and has content
        if os.path.exists(output_path):
            file_size = os.path.getsize(output_path)
            print(f"File size: {file_size} bytes")
            
            if file_size > 0:
                print("✅ File has content!")
                # Show first few lines
                with open(output_path, 'r') as f:
                    lines = f.readlines()
                    print(f"Number of lines: {len(lines)}")
                    if lines:
                        print(f"First line: {lines[0].strip()}")
                        if len(lines) > 1:
                            print(f"Second line: {lines[1].strip()}")
            else:
                print("❌ File is empty!")
        else:
            print("❌ Output file does not exist!")
            
    except Exception as e:
        print(f"❌ Error during full fetch: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("DEBUG: GitHub Actions Workflow Logic")
    print("="*50)
    
    test_rics_api()
    test_full_fetch()
