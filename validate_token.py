#!/usr/bin/env python3
"""
Simple script to validate RICS API token without running the full system.
"""

import os
import requests
from datetime import datetime, timedelta

def validate_rics_token():
    """Test if a RICS API token is valid."""
    
    token = os.getenv("RICS_API_TOKEN")
    if not token or token == "your_rics_api_token_here":
        print("❌ RICS_API_TOKEN not set!")
        print("\nTo test your token, run:")
        print("export RICS_API_TOKEN='your_actual_token_here'")
        print("python3 validate_token.py")
        return False
    
    print(f"🔍 Testing RICS API token (length: {len(token)})...")
    
    # Test with minimal API call
    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    headers = {"Token": token}
    
    # Very simple payload - just get 1 record from any store
    payload = {
        "Take": 1,
        "Skip": 0,
        "TicketDateStart": "2024-01-01T00:00:00Z",
        "TicketDateEnd": "2024-12-31T23:59:59Z",
        "BatchStartDate": "2024-01-01T00:00:00Z",
        "BatchEndDate": "2024-12-31T23:59:59Z",
        "StoreCode": "1"
    }
    
    try:
        print("📡 Making test API call...")
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        print(f"📊 Response Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            sales = data.get("Sales", [])
            print(f"✅ TOKEN IS VALID! API returned {len(sales)} records")
            print("🎉 Your token works - no need to contact RICS support")
            return True
            
        elif response.status_code == 401:
            print("❌ 401 Unauthorized - Token is invalid or expired")
            print("🔧 You need to get a new token from RICS support")
            return False
            
        else:
            print(f"❌ Unexpected error: {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            return False
            
    except Exception as e:
        print(f"❌ Connection error: {e}")
        print("🔧 This might be a network issue, not a token issue")
        return False

if __name__ == "__main__":
    validate_rics_token()

