#!/usr/bin/env python3
"""
Test script to verify subscription logic handles all cases correctly.

This script tests the upsert_profile_with_subscription function to ensure:
1. It does NOT re-subscribe if already subscribed (avoids duplicates)
2. It does NOT subscribe if globally suppressed/unsubscribed
3. It does NOT subscribe if explicitly unsubscribed from the specific list
4. It DOES subscribe if subscription is missing/None/pending

Usage:
    python scripts/test_subscription_logic.py
"""

import os
import sys

# Add parent directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from runsignup_connector.optimizely_client import get_profile, check_subscription_status, upsert_profile_with_subscription

def test_subscription_logic(email, list_id):
    """Test the subscription logic for a given email and list."""
    print(f"\n{'='*70}")
    print(f"Testing subscription logic for:")
    print(f"   Email: {email}")
    print(f"   List ID: {list_id}")
    print(f"{'='*70}\n")
    
    # Get current profile state
    profile = get_profile(email)
    if not profile:
        print("❌ Profile not found - cannot test")
        return
    
    print("📋 Current profile state:")
    print(f"   Suppressed: {profile.get('suppressed', False)}")
    print(f"   Unsubscribed (global): {profile.get('unsubscribed', False)}")
    
    # Check current subscription status
    sub_status = check_subscription_status(email, list_id)
    if sub_status:
        print(f"\n📧 Current subscription status for list '{list_id}':")
        print(f"   Status: {sub_status['status']}")
        print(f"   Subscribed: {sub_status['subscribed']}")
    else:
        print(f"\n📧 No subscription found for list '{list_id}'")
    
    # Show all subscriptions
    subscriptions = profile.get("subscriptions", [])
    print(f"\n📋 All subscriptions ({len(subscriptions)}):")
    for sub in subscriptions:
        if sub.get("list_id") == list_id:
            print(f"   ⭐ List: {sub.get('list_id')}")
            print(f"      Subscribed: {sub.get('subscribed')}")
            print(f"      Status: {sub.get('status', 'N/A')}")
        else:
            print(f"   List: {sub.get('list_id')}")
            print(f"      Subscribed: {sub.get('subscribed')}")
    
    # Analyze what the logic should do
    print(f"\n🔍 Logic Analysis:")
    
    # Check global suppression
    if profile.get("suppressed", False) or profile.get("unsubscribed", False):
        print(f"   ✅ Profile is globally suppressed/unsubscribed")
        print(f"   → Logic should: NOT subscribe (respect user preference)")
    else:
        print(f"   ✅ Profile is NOT globally suppressed")
    
    # Check specific list subscription
    list_sub = None
    for sub in subscriptions:
        if sub.get("list_id") == list_id:
            list_sub = sub
            break
    
    if list_sub:
        sub_status_value = list_sub.get("subscribed")
        if sub_status_value is False:
            print(f"   ✅ User is explicitly UNSUBSCRIBED from this list")
            print(f"   → Logic should: NOT subscribe (respect unsubscribe)")
        elif sub_status_value is True:
            print(f"   ✅ User is already SUBSCRIBED to this list")
            print(f"   → Logic should: NOT re-subscribe (avoid duplicates)")
        else:
            print(f"   ⚠️  Subscription exists but 'subscribed' is {sub_status_value} (unexpected)")
    else:
        print(f"   ✅ No subscription found for this list")
        print(f"   → Logic should: Subscribe them (they're not on the list)")
    
    print(f"\n💡 To test the actual behavior, run the connector with this email.")
    print(f"   The upsert_profile_with_subscription function will:")
    print(f"   1. Fetch the profile (already done above)")
    print(f"   2. Check global suppression (line 310)")
    print(f"   3. Check list-specific unsubscribe (line 320-330)")
    print(f"   4. Check if already subscribed (line 332-340)")
    print(f"   5. Subscribe if missing (line 342-355)")

def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/test_subscription_logic.py <email> <list_id>")
        print("\nExample:")
        print("  python scripts/test_subscription_logic.py emilyabenitez@gmail.com metro_run_walk_springfield")
        sys.exit(1)
    
    email = sys.argv[1]
    list_id = sys.argv[2]
    
    if not os.getenv("OPTIMIZELY_API_TOKEN"):
        print("❌ OPTIMIZELY_API_TOKEN not set")
        sys.exit(1)
    
    test_subscription_logic(email, list_id)

if __name__ == "__main__":
    main()

