#!/usr/bin/env python3
"""
Simple RICS API Test

Run this with your RICS token to test the API and see what data is available.
"""

import os
import requests
import json
from datetime import datetime, timedelta

def test_rics_simple():
    """Simple test of RICS API with your token."""
    
    print("🔍 SIMPLE RICS API TEST")
    print("=" * 40)
    
    # Get token from environment
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        print("❌ RICS_API_TOKEN not set")
        print("🔧 Set it with: export RICS_API_TOKEN='your_token_here'")
        return False
    
    print(f"✅ Token found (length: {len(token)})")
    
    # Test API with generous date range
    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    
    # Try last 30 days
    start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"🔍 Date range: {start_date} to {end_date}")
    
    # Test with store 1 first
    payload = {
        "Take": 10,
        "Skip": 0,
        "TicketDateStart": start_date,
        "TicketDateEnd": end_date,
        "BatchStartDate": start_date,
        "BatchEndDate": end_date,
        "StoreCode": "1"
    }
    
    try:
        print(f"🔍 Testing store 1...")
        resp = requests.post(url, headers={"Token": token}, json=payload, timeout=30)
        
        print(f"📊 Response status: {resp.status_code}")
        
        if resp.status_code == 200:
            data = resp.json()
            sales = data.get("Sales", [])
            print(f"✅ Success! Found {len(sales)} sales")
            
            if sales:
                print(f"\n🔍 First sale structure:")
                sale = sales[0]
                print(f"  Keys: {list(sale.keys())}")
                
                if 'SaleHeaders' in sale:
                    headers = sale['SaleHeaders']
                    print(f"  SaleHeaders count: {len(headers)}")
                    
                    if headers:
                        header = headers[0]
                        print(f"  First header keys: {list(header.keys())}")
                        
                        # Show the important fields
                        print(f"\n📊 Sample transaction:")
                        print(f"  TicketNumber: {header.get('TicketNumber')}")
                        print(f"  TicketDateTime: {header.get('TicketDateTime')}")
                        print(f"  SaleDateTime: {header.get('SaleDateTime')}")
                        print(f"  StoreCode: {sale.get('StoreCode')}")
                        
                        # Check customer data
                        customer = header.get('Customer', {})
                        if customer:
                            print(f"  Customer Email: {customer.get('Email')}")
                            print(f"  Customer Phone: {customer.get('Phone')}")
                            print(f"  Customer Name: {customer.get('FirstName')} {customer.get('LastName')}")
                        else:
                            print("  ⚠️ No customer data")
                        
                        # Check sale details
                        details = header.get('SaleDetails', [])
                        print(f"  SaleDetails count: {len(details)}")
                        
                        if details:
                            detail = details[0]
                            print(f"  First item: {detail.get('Sku')} - {detail.get('Summary')}")
                            print(f"  Quantity: {detail.get('Quantity')}")
                            print(f"  Amount: {detail.get('AmountPaid')}")
                
                return True
            else:
                print("⚠️ No sales found in response")
                return False
        else:
            print(f"❌ API Error {resp.status_code}")
            print(f"Response: {resp.text[:200]}")
            return False
            
    except Exception as e:
        print(f"❌ Exception: {e}")
        return False

def test_multiple_stores():
    """Test multiple stores to see which ones have data."""
    
    print(f"\n🔍 TESTING MULTIPLE STORES")
    print("=" * 40)
    
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        return
    
    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    start_date = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    stores = [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99]
    
    for store in stores:
        payload = {
            "Take": 1,
            "Skip": 0,
            "TicketDateStart": start_date,
            "TicketDateEnd": end_date,
            "BatchStartDate": start_date,
            "BatchEndDate": end_date,
            "StoreCode": str(store)
        }
        
        try:
            resp = requests.post(url, headers={"Token": token}, json=payload, timeout=10)
            
            if resp.status_code == 200:
                data = resp.json()
                sales = data.get("Sales", [])
                print(f"Store {store:2d}: {len(sales)} sales")
            else:
                print(f"Store {store:2d}: Error {resp.status_code}")
                
        except Exception as e:
            print(f"Store {store:2d}: Exception - {e}")

def main():
    print("🚀 SIMPLE RICS API TEST")
    print("=" * 40)
    print(f"Time: {datetime.now()}")
    print()
    
    # Test basic API
    success = test_rics_simple()
    
    if success:
        print("\n✅ API is working! Testing other stores...")
        test_multiple_stores()
        
        print(f"\n🔧 NEXT STEPS:")
        print(f"1. If you see data above, the API is working")
        print(f"2. Run: python3 scripts/sync_rics_live.py --debug")
        print(f"3. Check the output files in optimizely_connector/output/")
    else:
        print(f"\n❌ API test failed")
        print(f"🔧 Check your RICS_API_TOKEN and try again")

if __name__ == "__main__":
    main()
