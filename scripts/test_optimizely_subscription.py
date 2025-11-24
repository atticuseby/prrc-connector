#!/usr/bin/env python3
"""
Test script to debug Optimizely list subscription.

This script tests different methods of subscribing a profile to a list:
1. customer_update event with lists field (current approach)
2. /v3/profiles endpoint with subscriptions field (alternative)
3. list_subscribe event (fallback)

Usage:
    python scripts/test_optimizely_subscription.py <email> <list_id>
"""

import os
import sys
import json
import requests
from datetime import datetime, timezone

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN")
if not OPTIMIZELY_API_TOKEN:
    print("❌ OPTIMIZELY_API_TOKEN not set")
    sys.exit(1)

OPTIMIZELY_EVENTS_ENDPOINT = "https://api.zaius.com/v3/events"
OPTIMIZELY_PROFILES_ENDPOINT = "https://api.zaius.com/v3/profiles"

def get_headers():
    return {
        "x-api-key": OPTIMIZELY_API_TOKEN,
        "Content-Type": "application/json"
    }

def test_customer_update_with_lists(email, list_id):
    """Test method 1: customer_update event with lists field (current approach)"""
    print("\n" + "="*70)
    print("TEST 1: customer_update event with lists field")
    print("="*70)
    
    payload = {
        "type": "customer_update",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "identifiers": {
            "email": email
        },
        "properties": {
            "test_subscription": "true"
        },
        "lists": [{"id": list_id, "subscribe": True}]
    }
    
    print(f"📤 Sending payload:")
    print(json.dumps(payload, indent=2))
    
    try:
        response = requests.post(
            OPTIMIZELY_EVENTS_ENDPOINT,
            headers=get_headers(),
            json=payload,
            timeout=30
        )
        
        print(f"\n📥 Response:")
        print(f"   Status: {response.status_code}")
        print(f"   Headers: {dict(response.headers)}")
        
        try:
            response_json = response.json()
            print(f"   Body: {json.dumps(response_json, indent=2)}")
        except:
            print(f"   Body (text): {response.text[:500]}")
        
        return response.status_code, response.text
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, str(e)

def test_profiles_endpoint_with_subscriptions(email, list_id):
    """Test method 2: /v3/profiles endpoint with subscriptions field"""
    print("\n" + "="*70)
    print("TEST 2: /v3/profiles endpoint with subscriptions field")
    print("="*70)
    
    payload = {
        "identifiers": {
            "email": email
        },
        "attributes": {
            "test_subscription": "true"
        },
        "subscriptions": [
            {
                "list_id": list_id,
                "subscribed": True
            }
        ]
    }
    
    print(f"📤 Sending payload:")
    print(json.dumps(payload, indent=2))
    
    try:
        response = requests.post(
            OPTIMIZELY_PROFILES_ENDPOINT,
            headers=get_headers(),
            json=payload,
            timeout=30
        )
        
        print(f"\n📥 Response:")
        print(f"   Status: {response.status_code}")
        print(f"   Headers: {dict(response.headers)}")
        
        try:
            response_json = response.json()
            print(f"   Body: {json.dumps(response_json, indent=2)}")
        except:
            print(f"   Body (text): {response.text[:500]}")
        
        return response.status_code, response.text
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, str(e)

def test_list_subscribe_event(email, list_id):
    """Test method 3: list_subscribe event type"""
    print("\n" + "="*70)
    print("TEST 3: list_subscribe event type")
    print("="*70)
    
    payload = {
        "type": "list_subscribe",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "identifiers": {
            "email": email
        },
        "properties": {
            "list_id": list_id
        }
    }
    
    print(f"📤 Sending payload:")
    print(json.dumps(payload, indent=2))
    
    try:
        response = requests.post(
            OPTIMIZELY_EVENTS_ENDPOINT,
            headers=get_headers(),
            json=payload,
            timeout=30
        )
        
        print(f"\n📥 Response:")
        print(f"   Status: {response.status_code}")
        print(f"   Headers: {dict(response.headers)}")
        
        try:
            response_json = response.json()
            print(f"   Body: {json.dumps(response_json, indent=2)}")
        except:
            print(f"   Body (text): {response.text[:500]}")
        
        return response.status_code, response.text
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, str(e)

def check_profile_subscription(email, list_id):
    """Check current subscription status"""
    print("\n" + "="*70)
    print("CHECKING CURRENT SUBSCRIPTION STATUS")
    print("="*70)
    
    from runsignup_connector.optimizely_client import get_profile, check_subscription_status
    
    profile = get_profile(email)
    if not profile:
        print("❌ Profile not found")
        return
    
    print(f"✅ Profile found")
    print(f"   Suppressed: {profile.get('suppressed', False)}")
    print(f"   Unsubscribed (global): {profile.get('unsubscribed', False)}")
    
    sub_status = check_subscription_status(email, list_id)
    if sub_status:
        print(f"\n📧 Subscription status for list '{list_id}':")
        print(f"   Status: {sub_status['status']}")
        print(f"   Subscribed: {sub_status['subscribed']}")
    
    subscriptions = profile.get("subscriptions", [])
    print(f"\n📋 All subscriptions ({len(subscriptions)}):")
    for sub in subscriptions:
        print(f"   - List: {sub.get('list_id')}, Subscribed: {sub.get('subscribed')}, Status: {sub.get('status')}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/test_optimizely_subscription.py <email> <list_id>")
        print("\nExample:")
        print("  python scripts/test_optimizely_subscription.py emilyabenitez@gmail.com metro_run_walk_springfield")
        sys.exit(1)
    
    email = sys.argv[1]
    list_id = sys.argv[2]
    
    print(f"🧪 Testing Optimizely list subscription")
    print(f"   Email: {email}")
    print(f"   List ID: {list_id}")
    
    # Check current status first
    check_profile_subscription(email, list_id)
    
    # Test all three methods
    print("\n" + "="*70)
    print("RUNNING SUBSCRIPTION TESTS")
    print("="*70)
    print("\n⚠️  These tests will attempt to subscribe the profile.")
    print("   Wait a few minutes after each test before checking status.")
    
    # Test 1: customer_update with lists (current approach)
    status1, response1 = test_customer_update_with_lists(email, list_id)
    
    # Wait a moment
    import time
    print("\n⏳ Waiting 5 seconds before next test...")
    time.sleep(5)
    
    # Test 2: profiles endpoint with subscriptions
    status2, response2 = test_profiles_endpoint_with_subscriptions(email, list_id)
    
    # Wait a moment
    print("\n⏳ Waiting 5 seconds before next test...")
    time.sleep(5)
    
    # Test 3: list_subscribe event
    status3, response3 = test_list_subscribe_event(email, list_id)
    
    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)
    print(f"Test 1 (customer_update with lists): {status1}")
    print(f"Test 2 (profiles with subscriptions): {status2}")
    print(f"Test 3 (list_subscribe event): {status3}")
    
    print("\n💡 Next steps:")
    print(f"   1. Wait 3-5 minutes for Optimizely to process the events")
    print(f"   2. Run the diagnostic script to check subscription status:")
    print(f"      python scripts/check_optimizely_subscription.py {email} {list_id}")
    print(f"   3. If still not subscribed, verify the list ID is correct:")
    print(f"      python scripts/verify_optimizely_list.py {list_id}")
    print()
    print("📝 Note: Both Test 1 and Test 3 returned 202 (Accepted), which means")
    print("   Optimizely accepted the requests. If subscription doesn't appear after")
    print("   5 minutes, the list ID might be incorrect or there may be a delay.")

if __name__ == "__main__":
    main()

