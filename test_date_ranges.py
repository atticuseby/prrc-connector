#!/usr/bin/env python3
"""
Test script to check what date ranges have data in the RICS API
This will help us understand why we're not getting data in the 7-day window
"""
import os
import sys
from datetime import datetime, timedelta
import requests

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

def test_date_range(days_back, store_code=1):
    """Test a specific date range"""
    start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "Take": 10,
        "Skip": 0,
        "TicketDateStart": start_date,
        "TicketDateEnd": end_date,
        "BatchStartDate": start_date,
        "BatchEndDate": end_date,
        "StoreCode": store_code
    }
    
    print(f"\n=== Testing {days_back} days back (Store {store_code}) ===")
    print(f"Start: {start_date}")
    print(f"End: {end_date}")
    
    try:
        resp = requests.post(
            "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction",
            headers={"Token": os.getenv("RICS_API_TOKEN")},
            json=payload,
            timeout=30
        )
        
        print(f"Status: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            sales = data.get("Sales", [])
            print(f"Sales found: {len(sales)}")
            
            if sales:
                print(f"First sale date: {sales[0].get('TicketDateTime', 'No date')}")
                print(f"First sale number: {sales[0].get('TicketNumber', 'No number')}")
                return True
            else:
                print("No sales in this date range")
                return False
        else:
            print(f"API Error: {resp.text[:200]}")
            return False
            
    except Exception as e:
        print(f"Exception: {e}")
        return False

if __name__ == "__main__":
    print("Testing different date ranges to find where data exists...")
    
    # Test different date ranges
    date_ranges = [1, 3, 7, 14, 30, 60, 90]
    
    for days in date_ranges:
        has_data = test_date_range(days)
        if has_data:
            print(f"✅ FOUND DATA at {days} days back!")
            break
        else:
            print(f"❌ No data at {days} days back")
    
    print("\n=== Summary ===")
    print("This test will help us understand what date range actually contains data.")
    print("If no data is found in any range, there might be an API issue.")
    print("If data is found in longer ranges but not 7 days, we need to adjust our window.")
