#!/usr/bin/env python3
"""
Meta Offline Events Test Script

This script sends a test offline event to Meta to validate the integration
and help debug why Ads Manager isn't showing offline conversions.
"""

import os
import sys
import requests
import json
import hashlib
from datetime import datetime, timezone

def sha256_norm(value: str) -> str | None:
    """Hash a value with SHA256 for Meta matching."""
    if not value:
        return None
    v = value.strip().lower()
    if not v:
        return None
    return hashlib.sha256(v.encode("utf-8")).hexdigest()

def test_meta_connection():
    """Test basic connection to Meta API."""
    
    print("🔍 TESTING META API CONNECTION")
    print("=" * 50)
    
    # Check environment variables
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id = os.getenv("META_DATASET_ID")
    
    if not access_token:
        print("❌ META_ACCESS_TOKEN not set")
        return False
        
    if not dataset_id:
        print("❌ META_DATASET_ID not set")
        return False
    
    print(f"✅ Access token found (length: {len(access_token)})")
    print(f"✅ Dataset ID: {dataset_id}")
    
    # Test API endpoint
    api_url = f"https://graph.facebook.com/v19.0/{dataset_id}/events"
    
    # Create a test event
    test_email = os.getenv("TEST_EMAIL", "test@prrc-connector.com")
    current_time = int(datetime.now(timezone.utc).timestamp())
    
    test_event = {
        "event_name": "Purchase",
        "event_time": current_time,
        "action_source": "offline",  # CRITICAL: Must be "offline" for offline events
        "event_id": f"test-purchase-{current_time}",
        # NO event_source_url for offline events
        "user_data": {  # Use user_data, not match_keys
            "em": sha256_norm(test_email),
            "country": sha256_norm("US")
        },
        "custom_data": {
            "order_id": f"TEST-{current_time}",
            "value": 25.99,
            "currency": "USD",
            "contents": [{
                "id": "TEST-PRODUCT",
                "quantity": 1,
                "item_price": 25.99
            }]
        }
    }
    
    payload = {
        "data": [test_event],
        "access_token": access_token
    }
    
    print(f"🔍 Sending test event to: {api_url}")
    print(f"🔍 Test email: {test_email}")
    print(f"🔍 Event time: {datetime.fromtimestamp(current_time, tz=timezone.utc)}")
    
    try:
        resp = requests.post(
            api_url,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=30
        )
        
        print(f"📊 Response Status: {resp.status_code}")
        print(f"📊 Response Headers: {dict(resp.headers)}")
        
        if resp.status_code == 200:
            print("✅ Test event sent successfully!")
            response_data = resp.json()
            print(f"📊 Response: {json.dumps(response_data, indent=2)}")
            return True
        else:
            print(f"❌ API Error {resp.status_code}")
            print(f"Response: {resp.text}")
            return False
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return False

def test_dataset_info():
    """Get information about the dataset."""
    
    print("\n🔍 TESTING DATASET INFO")
    print("=" * 50)
    
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id = os.getenv("META_DATASET_ID")
    
    if not access_token or not dataset_id:
        print("❌ Missing required environment variables")
        return
    
    # Get dataset info
    info_url = f"https://graph.facebook.com/v19.0/{dataset_id}"
    
    try:
        resp = requests.get(
            info_url,
            params={"access_token": access_token},
            timeout=30
        )
        
        if resp.status_code == 200:
            data = resp.json()
            print("✅ Dataset info retrieved:")
            print(f"  ID: {data.get('id')}")
            print(f"  Name: {data.get('name')}")
            print(f"  Description: {data.get('description')}")
            print(f"  Data Source: {data.get('data_source')}")
            print(f"  Created Time: {data.get('created_time')}")
            print(f"  Updated Time: {data.get('updated_time')}")
        else:
            print(f"❌ Failed to get dataset info: {resp.status_code}")
            print(f"Response: {resp.text}")
            
    except Exception as e:
        print(f"❌ Error getting dataset info: {e}")

def test_offline_conversions():
    """Test if offline conversions are being tracked."""
    
    print("\n🔍 TESTING OFFLINE CONVERSIONS")
    print("=" * 50)
    
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id = os.getenv("META_DATASET_ID")
    
    if not access_token or not dataset_id:
        print("❌ Missing required environment variables")
        return
    
    # Get offline conversions for the dataset
    conversions_url = f"https://graph.facebook.com/v19.0/{dataset_id}/offline_conversions"
    
    try:
        resp = requests.get(
            conversions_url,
            params={"access_token": access_token},
            timeout=30
        )
        
        if resp.status_code == 200:
            data = resp.json()
            print("✅ Offline conversions info:")
            print(f"  Data: {json.dumps(data, indent=2)}")
        else:
            print(f"❌ Failed to get offline conversions: {resp.status_code}")
            print(f"Response: {resp.text}")
            
    except Exception as e:
        print(f"❌ Error getting offline conversions: {e}")

def main():
    print("🚀 META OFFLINE EVENTS TEST TOOL")
    print("=" * 50)
    
    # Test connection and send test event
    success = test_meta_connection()
    
    if success:
        print("\n✅ Meta API connection working!")
        
        # Get dataset info
        test_dataset_info()
        
        # Test offline conversions
        test_offline_conversions()
        
        print("\n🔧 NEXT STEPS:")
        print("1. Check Meta Event Manager for the test event")
        print("2. Check Ads Manager > Offline Conversions")
        print("3. Verify the dataset is properly attached to campaigns")
        print("4. Wait 15-30 minutes for data to process")
        
    else:
        print("\n❌ Meta API connection failed")
        print("\n🔧 TROUBLESHOOTING:")
        print("1. Check META_ACCESS_TOKEN is valid")
        print("2. Check META_DATASET_ID is correct")
        print("3. Verify token has offline_events permission")
        print("4. Check dataset is not deleted or disabled")
    
    print("\n" + "=" * 50)
    print("Test complete!")

if __name__ == "__main__":
    main()
