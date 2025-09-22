#!/usr/bin/env python3
"""
Comprehensive test script to verify all environment variables and API connections.
Run this after setting up your .env file to verify everything is working.
"""

import os
import sys
import requests
import json
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

def test_env_vars():
    """Test if all required environment variables are set."""
    print("🔍 Testing Environment Variables...")
    
    required_vars = [
        "RICS_API_TOKEN",
        "OPTIMIZELY_API_TOKEN", 
        "META_ACCESS_TOKEN",
        "META_DATASET_ID",
        "GDRIVE_CREDENTIALS",
        "GDRIVE_FOLDER_ID_RICS"
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value or value == f"your_{var.lower()}_here":
            missing_vars.append(var)
            print(f"❌ {var}: Not set or using placeholder value")
        else:
            print(f"✅ {var}: Set (length: {len(value)})")
    
    if missing_vars:
        print(f"\n⚠️ Missing or invalid variables: {', '.join(missing_vars)}")
        return False
    else:
        print("\n✅ All environment variables are properly configured!")
        return True

def test_rics_api():
    """Test RICS API connection."""
    print("\n🔍 Testing RICS API Connection...")
    
    token = os.getenv("RICS_API_TOKEN")
    if not token or token == "your_rics_api_token_here":
        print("❌ RICS_API_TOKEN not configured")
        return False
    
    # Test with a simple API call
    url = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
    headers = {"Token": token}
    
    # Test with minimal payload
    payload = {
        "Take": 1,
        "Skip": 0,
        "TicketDateStart": (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "TicketDateEnd": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "BatchStartDate": (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "BatchEndDate": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "StoreCode": 1
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        print(f"📡 RICS API Response: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            sales = data.get("Sales", [])
            print(f"✅ RICS API working! Found {len(sales)} sales records")
            return True
        elif response.status_code == 401:
            print("❌ RICS API: 401 Unauthorized - Check your RICS_API_TOKEN")
            return False
        else:
            print(f"❌ RICS API: Unexpected status {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            return False
            
    except Exception as e:
        print(f"❌ RICS API Error: {e}")
        return False

def test_optimizely_api():
    """Test Optimizely API connection."""
    print("\n🔍 Testing Optimizely API Connection...")
    
    token = os.getenv("OPTIMIZELY_API_TOKEN")
    if not token or token == "your_optimizely_api_token_here":
        print("❌ OPTIMIZELY_API_TOKEN not configured")
        return False
    
    # Test with a simple API call
    url = "https://api.zaius.com/v3/events"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Test payload
    test_payload = {
        "events": [{
            "type": "test_event",
            "timestamp": datetime.utcnow().isoformat(),
            "identifiers": {"email": "test@example.com"},
            "properties": {"test": True}
        }]
    }
    
    try:
        response = requests.post(url, headers=headers, json=test_payload, timeout=30)
        print(f"📡 Optimizely API Response: {response.status_code}")
        
        if response.status_code in [200, 201]:
            print("✅ Optimizely API working!")
            return True
        elif response.status_code == 401:
            print("❌ Optimizely API: 401 Unauthorized - Check your OPTIMIZELY_API_TOKEN")
            return False
        else:
            print(f"❌ Optimizely API: Unexpected status {response.status_code}")
            print(f"Response: {response.text[:200]}...")
            return False
            
    except Exception as e:
        print(f"❌ Optimizely API Error: {e}")
        return False

def test_google_drive():
    """Test Google Drive configuration."""
    print("\n🔍 Testing Google Drive Configuration...")
    
    creds_json = os.getenv("GDRIVE_CREDENTIALS")
    folder_id = os.getenv("GDRIVE_FOLDER_ID_RICS")
    
    if not creds_json or creds_json == "your_google_drive_credentials_json_here":
        print("❌ GDRIVE_CREDENTIALS not configured")
        return False
    
    if not folder_id or folder_id == "your_google_drive_folder_id_here":
        print("❌ GDRIVE_FOLDER_ID_RICS not configured")
        return False
    
    try:
        # Parse JSON to validate format
        creds_data = json.loads(creds_json)
        required_fields = ["type", "project_id", "private_key", "client_email"]
        missing_fields = [field for field in required_fields if field not in creds_data]
        
        if missing_fields:
            print(f"❌ Google Drive credentials missing fields: {missing_fields}")
            return False
        
        print("✅ Google Drive credentials format valid")
        print(f"✅ Google Drive folder ID: {folder_id}")
        return True
        
    except json.JSONDecodeError:
        print("❌ GDRIVE_CREDENTIALS is not valid JSON")
        return False
    except Exception as e:
        print(f"❌ Google Drive configuration error: {e}")
        return False

def test_meta_api():
    """Test Meta API configuration."""
    print("\n🔍 Testing Meta API Configuration...")
    
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id = os.getenv("META_DATASET_ID")
    
    if not access_token or access_token == "your_meta_access_token_here":
        print("❌ META_ACCESS_TOKEN not configured")
        return False
    
    if not dataset_id or dataset_id == "your_meta_dataset_id_here":
        print("❌ META_DATASET_ID not configured")
        return False
    
    print("✅ Meta API configuration present")
    print(f"✅ Dataset ID: {dataset_id}")
    return True

def main():
    """Run all tests."""
    print("🚀 PRRC Connector Setup Test")
    print("=" * 50)
    
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print("⚠️ python-dotenv not installed, using system environment variables")
    
    tests = [
        ("Environment Variables", test_env_vars),
        ("RICS API", test_rics_api),
        ("Optimizely API", test_optimizely_api),
        ("Google Drive", test_google_drive),
        ("Meta API", test_meta_api)
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"❌ {test_name} test failed with error: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 50)
    print("📊 TEST SUMMARY")
    print("=" * 50)
    
    passed = 0
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} {test_name}")
        if result:
            passed += 1
    
    print(f"\n🎯 {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! Your setup is ready to go.")
        print("\nNext steps:")
        print("1. Run: python3 scripts/sync_rics_live.py")
        print("2. Check the generated CSV files in optimizely_connector/output/")
        print("3. Verify data is uploaded to Google Drive")
    else:
        print("⚠️ Some tests failed. Please fix the issues above before proceeding.")
        print("\nCommon fixes:")
        print("- Edit .env file with your actual API tokens")
        print("- Ensure all API tokens are valid and not expired")
        print("- Check that Google Drive folder ID is correct")

if __name__ == "__main__":
    main()

