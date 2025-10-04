#!/usr/bin/env python3
"""
RICS API Token Diagnostic Script

This script helps diagnose RICS API token issues and provides guidance
on getting a fresh token if needed.
"""

import os
import sys
import requests
import json
from datetime import datetime, timedelta

def test_rics_token():
    """Test the current RICS API token and provide detailed diagnostics."""
    
    print("🔍 RICS API TOKEN DIAGNOSTIC")
    print("=" * 50)
    
    # Check if token exists
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        print("❌ RICS_API_TOKEN environment variable not set")
        print("🔧 ACTION: Set the RICS_API_TOKEN environment variable")
        return False
    
    print(f"✅ Token found (length: {len(token)})")
    print(f"🔍 Token preview: {token[:10]}...{token[-10:]}")
    
    # Test API endpoint
    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    
    # Test with minimal payload
    start_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
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
    
    print(f"🔍 Testing API endpoint: {url}")
    print(f"🔍 Date range: {start_date} to {end_date}")
    
    try:
        resp = requests.post(
            url,
            headers={"Token": token},
            json=payload,
            timeout=30
        )
        
        print(f"📊 Response Status: {resp.status_code}")
        print(f"📊 Response Headers: {dict(resp.headers)}")
        
        if resp.status_code == 401:
            print("❌ 401 Unauthorized - Token is invalid or expired")
            print("🔧 ACTION REQUIRED:")
            print("  1. Log into RICS Enterprise at https://enterprise.ricssoftware.com")
            print("  2. Go to Settings > API Keys")
            print("  3. Generate a new API token")
            print("  4. Update the RICS_API_TOKEN secret in GitHub")
            return False
            
        elif resp.status_code == 403:
            print("❌ 403 Forbidden - Token lacks required permissions")
            print("🔧 ACTION: Check token permissions in RICS Enterprise")
            return False
            
        elif resp.status_code == 429:
            print("⚠️ 429 Rate Limited - Too many requests")
            print("🔧 ACTION: Wait and retry later")
            return False
            
        elif resp.status_code != 200:
            print(f"❌ API Error {resp.status_code}")
            print(f"Response: {resp.text[:500]}")
            return False
        
        # Parse response
        try:
            data = resp.json()
            print("✅ API call successful!")
            
            sales = data.get("Sales", [])
            print(f"📊 Returned {len(sales)} sales")
            
            if sales:
                print("🔍 Sample sale structure:")
                sale = sales[0]
                print(f"  Keys: {list(sale.keys())}")
                
                if 'SaleHeaders' in sale:
                    headers = sale['SaleHeaders']
                    print(f"  SaleHeaders count: {len(headers)}")
                    if headers:
                        print(f"  First SaleHeader keys: {list(headers[0].keys())}")
                        
                        # Check for customer data
                        customer = headers[0].get('Customer', {})
                        if customer:
                            print(f"  Customer data: {list(customer.keys())}")
                            print(f"  Email: {customer.get('Email', 'N/A')}")
                            print(f"  Phone: {customer.get('Phone', 'N/A')}")
                        else:
                            print("  ⚠️ No customer data found")
            else:
                print("⚠️ No sales data returned")
                print("🔍 This could mean:")
                print("  - No transactions in the date range")
                print("  - Store code 1 has no recent activity")
                print("  - API permissions issue")
            
            return True
            
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON response: {e}")
            print(f"Raw response: {resp.text[:500]}")
            return False
            
    except requests.exceptions.Timeout:
        print("❌ Request timeout - API may be slow or unavailable")
        return False
        
    except requests.exceptions.ConnectionError:
        print("❌ Connection error - Check internet connection")
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_multiple_stores():
    """Test API with multiple store codes to see which ones have data."""
    
    print("\n🔍 TESTING MULTIPLE STORES")
    print("=" * 50)
    
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        print("❌ No token available for store testing")
        return
    
    stores = [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99]
    start_date = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    
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
            resp = requests.post(
                url,
                headers={"Token": token},
                json=payload,
                timeout=10
            )
            
            if resp.status_code == 200:
                data = resp.json()
                sales = data.get("Sales", [])
                print(f"Store {store:2d}: {len(sales)} sales")
            else:
                print(f"Store {store:2d}: Error {resp.status_code}")
                
        except Exception as e:
            print(f"Store {store:2d}: Exception - {e}")

def main():
    print("🚀 RICS API TOKEN DIAGNOSTIC TOOL")
    print("=" * 50)
    
    # Test current token
    success = test_rics_token()
    
    if success:
        print("\n✅ Token appears to be working!")
        test_multiple_stores()
    else:
        print("\n❌ Token needs to be updated")
        print("\n🔧 NEXT STEPS:")
        print("1. Get a fresh token from RICS Enterprise")
        print("2. Update the RICS_API_TOKEN secret in GitHub")
        print("3. Re-run the connector workflow")
    
    print("\n" + "=" * 50)
    print("Diagnostic complete!")

if __name__ == "__main__":
    main()
