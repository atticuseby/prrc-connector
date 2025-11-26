#!/usr/bin/env python3
"""
Diagnostic script to check what dates the RICS API actually returns.
This will help us understand if the API has a hard limit on recent data.
"""
import os
import sys
from datetime import datetime, timedelta
import requests
import json

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def parse_dt(dt_str):
    """Parse RICS date string."""
    if not dt_str:
        return None
    
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str.split('.')[0], fmt.replace('.%f', ''))
        except:
            continue
    
    return None

def test_api_date_range(days_back, store_code=1, take=100):
    """Test what dates the API actually returns for a given query range."""
    start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "Take": take,
        "Skip": 0,
        "TicketDateStart": start_date,
        "TicketDateEnd": end_date,
        "BatchStartDate": start_date,
        "BatchEndDate": end_date,
        "StoreCode": str(store_code)
    }
    
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        print("❌ RICS_API_TOKEN not found")
        return None
    
    try:
        resp = requests.post(
            "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction",
            headers={"Token": token},
            json=payload,
            timeout=30
        )
        
        if resp.status_code != 200:
            print(f"❌ API Error {resp.status_code}: {resp.text[:200]}")
            return None
        
        data = resp.json()
        sales = data.get("Sales", [])
        
        if not sales:
            return {
                "query_start": start_date,
                "query_end": end_date,
                "sales_count": 0,
                "dates_found": [],
                "oldest_date": None,
                "newest_date": None
            }
        
        # Extract all dates from the response
        all_dates = []
        for sale in sales:
            sale_headers = sale.get("SaleHeaders", [])
            for header in sale_headers:
                dt_str = header.get("TicketDateTime") or header.get("SaleDateTime")
                if dt_str:
                    dt = parse_dt(dt_str)
                    if dt:
                        all_dates.append(dt)
        
        if not all_dates:
            return {
                "query_start": start_date,
                "query_end": end_date,
                "sales_count": len(sales),
                "dates_found": [],
                "oldest_date": None,
                "newest_date": None
            }
        
        oldest = min(all_dates)
        newest = max(all_dates)
        
        return {
            "query_start": start_date,
            "query_end": end_date,
            "sales_count": len(sales),
            "dates_found": sorted(set(all_dates)),
            "oldest_date": oldest,
            "newest_date": newest,
            "days_old": (datetime.utcnow() - newest).days
        }
        
    except Exception as e:
        print(f"❌ Exception: {e}")
        return None

def main():
    print("=" * 80)
    print("RICS API Date Range Diagnostic")
    print("=" * 80)
    print(f"Current UTC time: {datetime.utcnow()}")
    print()
    
    # Test multiple date ranges to see what the API actually returns
    test_ranges = [1, 7, 14, 30, 45, 60, 90]
    
    print("Testing different query ranges to see what dates the API actually returns:")
    print()
    
    results = []
    for days_back in test_ranges:
        print(f"Testing {days_back} day lookback...", end=" ")
        result = test_api_date_range(days_back, store_code=1, take=200)
        
        if result:
            if result["newest_date"]:
                print(f"✅ Found data up to {result['newest_date']} ({result['days_old']} days old)")
                print(f"   Query: {result['query_start']} to {result['query_end']}")
                print(f"   Actual data: {result['oldest_date']} to {result['newest_date']}")
            else:
                print(f"⚠️  No dates found in response")
            results.append(result)
        else:
            print(f"❌ Failed")
    
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    # Find the absolute newest date across all queries
    all_newest_dates = [r["newest_date"] for r in results if r and r.get("newest_date")]
    
    if all_newest_dates:
        absolute_newest = max(all_newest_dates)
        days_old = (datetime.utcnow() - absolute_newest).days
        
        print(f"🎯 Most recent date found in ANY query: {absolute_newest}")
        print(f"   This is {days_old} days old")
        print()
        
        if days_old > 7:
            print(f"⚠️  CONCLUSION: RICS API appears to have a {days_old}-day delay.")
            print(f"   The API is NOT returning data newer than {absolute_newest}, regardless of query range.")
            print(f"   This is an API limitation, not a code issue.")
        else:
            print(f"✅ API is returning recent data (within 7 days)")
        
        # Show which queries found the newest data
        print()
        print("Queries that found the newest data:")
        for r in results:
            if r and r.get("newest_date") == absolute_newest:
                query_days = (datetime.utcnow() - parse_dt(r["query_start"])).days
                print(f"   - {query_days} day lookback query")
    else:
        print("❌ No dates found in any query!")
        print("   This could indicate:")
        print("   - API token issue")
        print("   - API endpoint issue")
        print("   - No sales data available")

if __name__ == "__main__":
    sys.exit(main() if main() else 0)

