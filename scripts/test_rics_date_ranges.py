#!/usr/bin/env python3
"""
Test script to check what date ranges have data in the RICS API.
Tests multiple date ranges in parallel to find the most recent sale.
"""
import os
import sys
from datetime import datetime, timedelta
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def test_date_range(days_back, store_code=1):
    """Test a specific date range and return the most recent sale date found."""
    start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "Take": 100,  # Get more results to find the most recent
        "Skip": 0,
        "TicketDateStart": start_date,
        "TicketDateEnd": end_date,
        "BatchStartDate": start_date,
        "BatchEndDate": end_date,
        "StoreCode": str(store_code)
    }
    
    try:
        resp = requests.post(
            "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction",
            headers={"Token": os.getenv("RICS_API_TOKEN")},
            json=payload,
            timeout=30
        )
        
        if resp.status_code == 200:
            data = resp.json()
            sales = data.get("Sales", [])
            
            if sales:
                # Find the most recent sale across all SaleHeaders
                most_recent_date = None
                most_recent_ticket = None
                
                for sale in sales:
                    sale_headers = sale.get("SaleHeaders", [])
                    for header in sale_headers:
                        ticket_dt_str = header.get("TicketDateTime") or header.get("SaleDateTime")
                        if ticket_dt_str:
                            try:
                                # Try to parse the date
                                ticket_dt = datetime.strptime(ticket_dt_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                                if most_recent_date is None or ticket_dt > most_recent_date:
                                    most_recent_date = ticket_dt
                                    most_recent_ticket = header.get("TicketNumber")
                            except:
                                pass
                
                return {
                    "days_back": days_back,
                    "start_date": start_date,
                    "end_date": end_date,
                    "sales_count": len(sales),
                    "most_recent_date": most_recent_date,
                    "most_recent_ticket": most_recent_ticket,
                    "status": "success"
                }
            else:
                return {
                    "days_back": days_back,
                    "start_date": start_date,
                    "end_date": end_date,
                    "sales_count": 0,
                    "status": "no_data"
                }
        else:
            return {
                "days_back": days_back,
                "start_date": start_date,
                "end_date": end_date,
                "status": "error",
                "status_code": resp.status_code,
                "error": resp.text[:200]
            }
            
    except Exception as e:
        return {
            "days_back": days_back,
            "start_date": start_date,
            "end_date": end_date,
            "status": "exception",
            "error": str(e)
        }

def main():
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        print("❌ RICS_API_TOKEN not found in environment")
        return 1
    
    print("🔍 Testing multiple date ranges to find most recent RICS sales...")
    print(f"📅 Current UTC time: {datetime.utcnow()}")
    print()
    
    # Test multiple date ranges in parallel
    # Test: last 7 days, 14 days, 30 days, 45 days, 60 days, 90 days
    date_ranges = [7, 14, 30, 45, 60, 90]
    
    # Also test specific recent ranges: last 1 day, 2 days, 3 days, 5 days
    recent_ranges = [1, 2, 3, 5]
    
    all_ranges = recent_ranges + date_ranges
    
    results = []
    
    print("📊 Testing date ranges in parallel...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(test_date_range, days_back, store_code=1): days_back
            for days_back in all_ranges
        }
        
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            
            if result["status"] == "success":
                if result["sales_count"] > 0:
                    print(f"✅ {result['days_back']:3d} days back: {result['sales_count']:3d} sales, "
                          f"most recent: {result['most_recent_date']} (ticket {result['most_recent_ticket']})")
                else:
                    print(f"⚠️  {result['days_back']:3d} days back: No sales found")
            elif result["status"] == "no_data":
                print(f"⚠️  {result['days_back']:3d} days back: No sales in response")
            else:
                print(f"❌ {result['days_back']:3d} days back: {result['status']} - {result.get('error', 'Unknown error')}")
    
    print()
    print("=" * 80)
    print("📊 SUMMARY")
    print("=" * 80)
    
    # Find the most recent sale across all tests
    successful_results = [r for r in results if r["status"] == "success" and r.get("most_recent_date")]
    
    if successful_results:
        most_recent = max(successful_results, key=lambda x: x["most_recent_date"])
        print(f"🎯 Most recent sale found:")
        print(f"   Date: {most_recent['most_recent_date']}")
        print(f"   Ticket: {most_recent['most_recent_ticket']}")
        print(f"   Found in: {most_recent['days_back']} day lookback")
        print()
        
        # Show which ranges have data
        print("📈 Data availability by date range:")
        for result in sorted(results, key=lambda x: x["days_back"]):
            if result["status"] == "success" and result["sales_count"] > 0:
                print(f"   {result['days_back']:3d} days: ✅ {result['sales_count']:3d} sales")
            elif result["status"] == "no_data":
                print(f"   {result['days_back']:3d} days: ⚠️  No data")
            else:
                print(f"   {result['days_back']:3d} days: ❌ Error")
        
        # Calculate how many days old the most recent sale is
        days_old = (datetime.utcnow() - most_recent["most_recent_date"]).days
        print()
        print(f"⚠️  Most recent sale is {days_old} days old")
        if days_old > 7:
            print(f"   This suggests the API may not be returning recent data, or there's a delay in data availability.")
    else:
        print("❌ No sales found in any date range tested!")
        print("   This could indicate:")
        print("   - API token issue")
        print("   - API endpoint issue")
        print("   - No recent sales data available")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

