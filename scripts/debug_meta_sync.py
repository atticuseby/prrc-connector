#!/usr/bin/env python3
"""
Meta Sync Diagnostic Script
This script helps diagnose issues with the RICS to Meta sync
"""

import os
import sys
import requests
import json
from datetime import datetime

def check_environment():
    """Check if required environment variables are set"""
    print("🔍 Checking environment variables...")
    
    required_vars = {
        "META_OFFLINE_SET_ID": os.environ.get("META_OFFLINE_SET_ID"),
        "META_OFFLINE_TOKEN": os.environ.get("META_OFFLINE_TOKEN"),
        "RICS_CSV_PATH": os.environ.get("RICS_CSV_PATH", "./data/rics.csv")
    }
    
    missing = []
    for var, value in required_vars.items():
        if value:
            if var == "META_OFFLINE_TOKEN":
                print(f"   ✅ {var}: {'*' * 10}{value[-4:] if len(value) > 4 else '*' * len(value)}")
            else:
                print(f"   ✅ {var}: {value}")
        else:
            print(f"   ❌ {var}: NOT SET")
            missing.append(var)
    
    if missing:
        print(f"\n❌ Missing required variables: {', '.join(missing)}")
        return False
    
    print("✅ All environment variables are set")
    return True

def test_meta_api():
    """Test Meta API connectivity and permissions"""
    print("\n🔍 Testing Meta API...")
    
    offline_set_id = os.environ.get("META_OFFLINE_SET_ID")
    access_token = os.environ.get("META_OFFLINE_TOKEN")
    
    # Test 1: Basic API access
    print("   Testing basic API access...")
    try:
        url = "https://graph.facebook.com/v16.0/me"
        params = {"access_token": access_token}
        resp = requests.get(url, params=params, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"   ✅ API access OK - User: {data.get('name', 'Unknown')}")
        else:
            print(f"   ❌ API access failed: {resp.status_code}")
            print(f"      Response: {resp.text}")
            return False
    except Exception as e:
        print(f"   ❌ API connection failed: {e}")
        return False
    
    # Test 2: Offline Event Set access
    print("   Testing Offline Event Set access...")
    try:
        url = f"https://graph.facebook.com/v16.0/{offline_set_id}"
        params = {"access_token": access_token}
        resp = requests.get(url, params=params, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            print(f"   ✅ Offline Event Set found: {data.get('name', 'Unknown')}")
            print(f"      ID: {data.get('id')}")
            print(f"      Description: {data.get('description', 'No description')}")
            return True
        else:
            print(f"   ❌ Offline Event Set access failed: {resp.status_code}")
            print(f"      Response: {resp.text}")
            
            # Try to parse error
            try:
                error_data = resp.json()
                if "error" in error_data:
                    error = error_data["error"]
                    print(f"      Error Code: {error.get('code')}")
                    print(f"      Error Message: {error.get('message')}")
                    
                    if error.get('code') == 100:
                        print("      💡 Solution: Add 'ads_management' permission to your access token")
                    elif error.get('code') == 190:
                        print("      💡 Solution: Generate a new access token")
                    elif error.get('code') == 294:
                        print("      💡 Solution: Check if the Offline Event Set ID is correct")
            except:
                pass
            
            return False
    except Exception as e:
        print(f"   ❌ Offline Event Set test failed: {e}")
        return False

def test_sample_event():
    """Test sending a single sample event"""
    print("\n🧪 Testing sample event upload...")
    
    offline_set_id = os.environ.get("META_OFFLINE_SET_ID")
    access_token = os.environ.get("META_OFFLINE_TOKEN")
    
    # Create a test event
    test_event = {
        "event_name": "Purchase",
        "event_time": int(datetime.now().timestamp()),
        "event_id": f"test_event_{int(datetime.now().timestamp())}",
        "user_data": {
            "em": "test@example.com",  # This should be hashed in production
            "ph": "1234567890"         # This should be hashed in production
        },
        "custom_data": {
            "value": 10.00,
            "currency": "USD"
        }
    }
    
    url = f"https://graph.facebook.com/v16.0/{offline_set_id}/events"
    payload = {"data": [test_event]}
    params = {"access_token": access_token}
    
    try:
        print("   Sending test event...")
        resp = requests.post(url, json=payload, params=params, timeout=30)
        
        if resp.status_code == 200:
            result = resp.json()
            print(f"   ✅ Test event successful!")
            print(f"      Events received: {result.get('events_received', 0)}")
            return True
        else:
            print(f"   ❌ Test event failed: {resp.status_code}")
            print(f"      Response: {resp.text}")
            
            # Parse error details
            try:
                error_data = resp.json()
                if "error" in error_data:
                    error = error_data["error"]
                    print(f"      Error Code: {error.get('code')}")
                    print(f"      Error Message: {error.get('message')}")
                    
                    if error.get('code') == 100:
                        print("      💡 Solution: Check access token permissions")
                    elif error.get('code') == 190:
                        print("      💡 Solution: Invalid access token")
                    elif error.get('code') == 294:
                        print("      💡 Solution: Check offline event set configuration")
                    elif error.get('code') == 100:
                        print("      💡 Solution: Check event data format")
            except:
                pass
            
            return False
    except Exception as e:
        print(f"   ❌ Test event failed: {e}")
        return False

def check_csv_file():
    """Check if CSV file exists and show sample data"""
    print("\n📋 Checking CSV file...")
    
    csv_path = os.environ.get("RICS_CSV_PATH", "./data/rics.csv")
    
    if not os.path.exists(csv_path):
        print(f"   ❌ CSV file not found: {csv_path}")
        return False
    
    print(f"   ✅ CSV file found: {csv_path}")
    
    # Check file size
    file_size = os.path.getsize(csv_path)
    print(f"   📊 File size: {file_size:,} bytes")
    
    # Show first few lines
    try:
        import csv
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            print(f"   📋 Headers: {headers}")
            
            # Count rows
            rows = list(reader)
            print(f"   📊 Total rows: {len(rows)}")
            
            if rows:
                print("   📝 Sample row:")
                sample = rows[0]
                for key, value in sample.items():
                    if key in ["email", "phone"]:
                        # Mask sensitive data
                        if value:
                            masked = value[:3] + "*" * (len(value) - 6) + value[-3:] if len(value) > 6 else "*" * len(value)
                            print(f"      {key}: {masked}")
                        else:
                            print(f"      {key}: (empty)")
                    else:
                        print(f"      {key}: {value}")
    
    except Exception as e:
        print(f"   ❌ Error reading CSV: {e}")
        return False
    
    return True

def main():
    print("🔧 Meta Sync Diagnostic Tool")
    print("=" * 40)
    
    # Check environment
    if not check_environment():
        print("\n❌ Environment check failed - please set required variables")
        sys.exit(1)
    
    # Test Meta API
    if not test_meta_api():
        print("\n❌ Meta API test failed - check credentials and permissions")
        sys.exit(1)
    
    # Test sample event
    if not test_sample_event():
        print("\n❌ Sample event test failed - check event format and permissions")
        sys.exit(1)
    
    # Check CSV file
    if not check_csv_file():
        print("\n❌ CSV file check failed")
        sys.exit(1)
    
    print("\n🎉 All diagnostic tests passed!")
    print("   Your Meta sync should work correctly now.")
    print("\n💡 Next steps:")
    print("   1. Run the full sync: python scripts/sync_rics_to_meta.py")
    print("   2. Check the logs for any issues")
    print("   3. Verify events appear in Meta Events Manager")

if __name__ == "__main__":
    main() 