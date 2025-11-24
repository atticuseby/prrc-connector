#!/usr/bin/env python3
"""
Diagnostic script to check Optimizely subscription status for a specific email and list.

Usage:
    python scripts/check_optimizely_subscription.py <email> <list_id>

Example:
    python scripts/check_optimizely_subscription.py emilyabenitez@gmail.com =pr=_training_news
"""

import os
import sys

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from runsignup_connector.optimizely_client import get_profile, check_subscription_status, subscribe_to_list
import json


def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/check_optimizely_subscription.py <email> <list_id>")
        print("\nExample:")
        print("  python scripts/check_optimizely_subscription.py emilyabenitez@gmail.com =pr=_training_news")
        sys.exit(1)
    
    email = sys.argv[1]
    list_id = sys.argv[2]
    
    print(f"🔍 Checking subscription status for:")
    print(f"   Email: {email}")
    print(f"   List ID: {list_id}")
    print()
    
    # Check if API token is set
    if not os.getenv("OPTIMIZELY_API_TOKEN"):
        print("❌ ERROR: OPTIMIZELY_API_TOKEN environment variable is not set")
        sys.exit(1)
    
    # Get full profile
    print("📋 Fetching profile...")
    profile = get_profile(email)
    
    if not profile:
        print(f"❌ Profile not found for {email}")
        print("\n💡 This means the profile doesn't exist in Optimizely yet.")
        print("   The connector will create it on the next run.")
        sys.exit(1)
    
    print(f"✅ Profile found")
    print(f"   Profile ID: {profile.get('zid', 'N/A')}")
    print(f"   Suppressed: {profile.get('suppressed', False)}")
    print(f"   Unsubscribed (global): {profile.get('unsubscribed', False)}")
    print()
    
    # Check subscription status
    print(f"📧 Checking subscription to list: {list_id}")
    sub_status = check_subscription_status(email, list_id)
    
    if sub_status:
        print(f"   Status: {sub_status['status']}")
        print(f"   Subscribed: {sub_status['subscribed']}")
        print(f"   Profile suppressed: {sub_status['profile_suppressed']}")
        print(f"   Profile unsubscribed: {sub_status['profile_unsubscribed']}")
        print()
        
        if sub_status['status'] == 'not_found':
            print("⚠️  Subscription not found for this list")
            print("   This could mean:")
            print("   - The profile was never subscribed to this list")
            print("   - The subscription event hasn't been processed yet")
            print("   - There's an issue with the list_id")
            print()
            print("💡 To subscribe this profile, run:")
            print(f"   python -c \"from runsignup_connector.optimizely_client import subscribe_to_list; import os; os.environ['OPTIMIZELY_API_TOKEN'] = '{os.getenv('OPTIMIZELY_API_TOKEN')[:10]}...'; print(subscribe_to_list('{email}', '{list_id}'))\"")
        elif sub_status['subscribed']:
            print("✅ Profile is subscribed to this list")
        else:
            print("❌ Profile is NOT subscribed (unsubscribed or suppressed)")
    else:
        print("❌ Could not check subscription status")
    
    # Show all subscriptions
    print()
    print("📋 All subscriptions for this profile:")
    subscriptions = profile.get("subscriptions", [])
    if subscriptions:
        for sub in subscriptions:
            print(f"   List: {sub.get('list_id', 'N/A')}")
            print(f"      Subscribed: {sub.get('subscribed', 'N/A')}")
            print(f"      Status: {sub.get('status', 'N/A')}")
    else:
        print("   No subscriptions found")
    
    # Show recent events (if available in profile)
    print()
    print("📊 Profile attributes (sample):")
    attrs = {k: v for k, v in profile.items() if k not in ['subscriptions', 'zid', 'suppressed', 'unsubscribed']}
    for key, value in list(attrs.items())[:10]:
        print(f"   {key}: {value}")


if __name__ == "__main__":
    main()




