#!/usr/bin/env python3
"""
Verify that an Optimizely list ID exists and is accessible.

Usage:
    python scripts/verify_optimizely_list.py <list_id>
"""

import os
import sys
import requests

OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN")
if not OPTIMIZELY_API_TOKEN:
    print("❌ OPTIMIZELY_API_TOKEN not set")
    sys.exit(1)

def verify_list(list_id):
    """Try to verify list exists by attempting to get list info."""
    # Optimizely doesn't have a direct "get list" endpoint in v3
    # But we can try to subscribe a test profile and see if it works
    
    print(f"🔍 Verifying list ID: {list_id}")
    print(f"   (Optimizely API doesn't expose a direct list lookup endpoint)")
    print()
    
    # Try to get a profile and check if we can see any list subscriptions
    # This is indirect, but if the list ID is wrong, subscriptions won't work
    
    print("💡 To verify the list ID is correct:")
    print("   1. Check the Optimizely UI - the list ID should be in the URL")
    print("   2. The list ID format is usually: lowercase_with_underscores")
    print("   3. Wait 3-5 minutes after subscribing, then check the profile")
    print()
    
    # Check if we can at least make an API call with this list ID
    test_payload = {
        "type": "list_subscribe",
        "identifiers": {
            "email": "test@example.com"  # Fake email, won't actually subscribe
        },
        "properties": {
            "list_id": list_id
        }
    }
    
    try:
        response = requests.post(
            "https://api.zaius.com/v3/events",
            headers={
                "x-api-key": OPTIMIZELY_API_TOKEN,
                "Content-Type": "application/json"
            },
            json=test_payload,
            timeout=10
        )
        
        if response.status_code == 202:
            print(f"✅ API accepts the list ID (status 202)")
            print(f"   This suggests the list ID format is valid")
        elif response.status_code == 400:
            print(f"⚠️  API returned 400 (Bad Request)")
            print(f"   This might indicate the list ID is invalid")
            try:
                error = response.json()
                print(f"   Error: {error}")
            except:
                print(f"   Error text: {response.text[:200]}")
        else:
            print(f"⚠️  Unexpected status: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            
    except Exception as e:
        print(f"❌ Error testing list ID: {e}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/verify_optimizely_list.py <list_id>")
        print("\nExample:")
        print("  python scripts/verify_optimizely_list.py metro_run_walk_springfield")
        sys.exit(1)
    
    list_id = sys.argv[1]
    verify_list(list_id)

if __name__ == "__main__":
    main()

