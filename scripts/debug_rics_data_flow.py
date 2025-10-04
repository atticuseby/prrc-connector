#!/usr/bin/env python3
"""
RICS Data Flow Debug Script

This script helps debug why no data is being fetched from RICS API
even when the token is valid.
"""

import os
import sys
import requests
import json
from datetime import datetime, timedelta

def test_rics_api_detailed():
    """Test RICS API with detailed debugging for data flow issues."""
    
    print("🔍 RICS API DETAILED DEBUG")
    print("=" * 60)
    
    # Check token
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        print("❌ RICS_API_TOKEN not set")
        print("🔧 Set it with: export RICS_API_TOKEN='your_token_here'")
        return False
    
    print(f"✅ Token found (length: {len(token)})")
    
    # Test different date ranges
    date_ranges = [
        ("Last 1 day", 1),
        ("Last 3 days", 3), 
        ("Last 7 days", 7),
        ("Last 14 days", 14),
        ("Last 30 days", 30)
    ]
    
    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    
    for range_name, days in date_ranges:
        print(f"\n🔍 Testing {range_name}...")
        
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        print(f"  Date range: {start_date} to {end_date}")
        
        # Test with different store codes
        test_stores = [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99]
        
        for store in test_stores:
            payload = {
                "Take": 10,  # Get more records for better testing
                "Skip": 0,
                "TicketDateStart": start_date,
                "TicketDateEnd": end_date,
                "BatchStartDate": start_date,
                "BatchEndDate": end_date,
                "StoreCode": str(store)
            }
            
            try:
                resp = requests.post(
                    url,
                    headers={"Token": token},
                    json=payload,
                    timeout=30
                )
                
                if resp.status_code == 200:
                    data = resp.json()
                    sales = data.get("Sales", [])
                    
                    if sales:
                        print(f"  ✅ Store {store:2d}: {len(sales)} sales found")
                        
                        # Analyze the first sale
                        sale = sales[0]
                        print(f"    Sale keys: {list(sale.keys())}")
                        
                        if 'SaleHeaders' in sale:
                            headers = sale['SaleHeaders']
                            print(f"    SaleHeaders: {len(headers)}")
                            
                            if headers:
                                header = headers[0]
                                print(f"    First header keys: {list(header.keys())}")
                                
                                # Check dates
                                ticket_dt = header.get('TicketDateTime')
                                sale_dt = header.get('SaleDateTime')
                                print(f"    TicketDateTime: {ticket_dt}")
                                print(f"    SaleDateTime: {sale_dt}")
                                
                                # Check customer data
                                customer = header.get('Customer', {})
                                if customer:
                                    print(f"    Customer data: {list(customer.keys())}")
                                    print(f"    Email: {customer.get('Email', 'N/A')}")
                                    print(f"    Phone: {customer.get('Phone', 'N/A')}")
                                else:
                                    print("    ⚠️ No customer data")
                                
                                # Check sale details
                                details = header.get('SaleDetails', [])
                                print(f"    SaleDetails: {len(details)}")
                                
                                if details:
                                    detail = details[0]
                                    print(f"    First detail: {detail}")
                                
                                # Found data, no need to test more stores for this date range
                                return True
                    else:
                        print(f"  ⚠️ Store {store:2d}: No sales")
                else:
                    print(f"  ❌ Store {store:2d}: Error {resp.status_code}")
                    if resp.status_code == 401:
                        print("    🔧 Token appears to be invalid!")
                        return False
                        
            except Exception as e:
                print(f"  ❌ Store {store:2d}: Exception - {e}")
        
        print(f"  📊 No data found in {range_name}")
    
    print("\n❌ No data found in any date range or store")
    return False

def test_different_endpoints():
    """Test if there are other RICS API endpoints that might work."""
    
    print("\n🔍 TESTING DIFFERENT RICS ENDPOINTS")
    print("=" * 60)
    
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        return False
    
    # Test different endpoints
    endpoints = [
        "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction",
        "https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory",
        "https://enterprise.ricssoftware.com/api/POS/GetSales",
        "https://enterprise.ricssoftware.com/api/POS/GetTransactions"
    ]
    
    for endpoint in endpoints:
        print(f"\n🔍 Testing endpoint: {endpoint}")
        
        payload = {
            "Take": 1,
            "Skip": 0,
            "StoreCode": "1"
        }
        
        try:
            resp = requests.post(
                endpoint,
                headers={"Token": token},
                json=payload,
                timeout=30
            )
            
            print(f"  Status: {resp.status_code}")
            
            if resp.status_code == 200:
                data = resp.json()
                print(f"  ✅ Success! Response keys: {list(data.keys())}")
                
                # Check for sales data
                if 'Sales' in data:
                    sales = data['Sales']
                    print(f"  Sales count: {len(sales)}")
                elif 'Data' in data:
                    data_items = data['Data']
                    print(f"  Data count: {len(data_items)}")
                else:
                    print(f"  Response structure: {json.dumps(data, indent=2)[:200]}...")
            else:
                print(f"  ❌ Error: {resp.text[:100]}")
                
        except Exception as e:
            print(f"  ❌ Exception: {e}")

def test_date_field_parsing():
    """Test different date field parsing approaches."""
    
    print("\n🔍 TESTING DATE FIELD PARSING")
    print("=" * 60)
    
    # Test different date formats that might be returned
    test_dates = [
        "2025-10-03T10:30:00Z",
        "2025-10-03T10:30:00",
        "2025-10-03 10:30:00",
        "10/03/2025 10:30:00",
        "2025-10-03T10:30:00.000Z",
        "2025-10-03T10:30:00.000"
    ]
    
    from datetime import datetime
    
    for date_str in test_dates:
        print(f"\nTesting date: {date_str}")
        
        # Try different parsing formats
        formats = [
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%S.%f"
        ]
        
        parsed = False
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str, fmt)
                print(f"  ✅ Parsed with {fmt}: {dt}")
                parsed = True
                break
            except:
                continue
        
        if not parsed:
            print(f"  ❌ Could not parse")

def main():
    print("🚀 RICS DATA FLOW DEBUG TOOL")
    print("=" * 60)
    print(f"Timestamp: {datetime.now()}")
    
    # Test 1: Detailed API testing
    success1 = test_rics_api_detailed()
    
    if success1:
        print("\n✅ Found data! The API is working.")
        print("🔧 The issue might be in the data processing pipeline.")
    else:
        print("\n❌ No data found in API calls.")
        print("🔧 Testing alternative endpoints...")
        test_different_endpoints()
    
    # Test 2: Date parsing
    test_date_field_parsing()
    
    print("\n" + "=" * 60)
    print("DEBUG COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()
