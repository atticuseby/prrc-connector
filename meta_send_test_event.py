#!/usr/bin/env python3
"""
Meta Send Test Event

Sends a single test offline event to Meta to validate the integration
and ensure proper offline Conversions API format.
"""

import os
import json
import hashlib
import requests
from datetime import datetime, timezone, timedelta

def sha256_norm(value: str) -> str | None:
    """Hash a value with SHA256 for Meta matching."""
    if not value:
        return None
    v = value.strip().lower()
    if not v:
        return None
    return hashlib.sha256(v.encode("utf-8")).hexdigest()

def main():
    print("🚀 META SEND TEST EVENT")
    print("=" * 50)
    
    # Get credentials from environment
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id = os.getenv("META_DATASET_ID")
    test_email = os.getenv("TEST_EMAIL", "test@prrc-connector.com")
    
    if not access_token:
        print("❌ META_ACCESS_TOKEN not set")
        print("🔧 Set it with: export META_ACCESS_TOKEN='your_token_here'")
        return 1
        
    if not dataset_id:
        print("❌ META_DATASET_ID not set")
        print("🔧 Set it with: export META_DATASET_ID='your_dataset_id_here'")
        return 1
    
    print(f"✅ Access token found (length: {len(access_token)})")
    print(f"✅ Dataset ID: {dataset_id}")
    print(f"✅ Test email: {test_email}")
    
    # Create test event with proper offline format
    current_time = int(datetime.now(timezone.utc).timestamp())
    test_time = current_time - 600  # 10 minutes ago
    
    # Hash the test email
    hashed_email = sha256_norm(test_email)
    
    test_event = {
        "event_name": "Purchase",
        "event_time": test_time,
        "action_source": "offline",  # CRITICAL: Must be "offline"
        "event_id": f"test-purchase-{current_time}",
        # NO event_source_url for offline events
        "user_data": {
            "em": hashed_email,
            "country": sha256_norm("US")
        },
        "custom_data": {
            "order_id": f"TEST-{current_time}",
            "value": 1.23,
            "currency": "USD"
        }
    }
    
    # Optional test event code for manual testing
    test_event_code = os.getenv("TEST_EVENT_CODE")
    if test_event_code:
        test_event["test_event_code"] = test_event_code
        print(f"🔍 Using test event code: {test_event_code}")
    
    # Prepare payload
    payload = {
        "data": [test_event],
        "access_token": access_token,
        "upload_tag": f"test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    }
    
    # Send to Meta
    api_url = f"https://graph.facebook.com/v19.0/{dataset_id}/events"
    
    print(f"\n🔍 Sending test event to: {api_url}")
    print(f"🔍 Event time: {datetime.fromtimestamp(test_time, tz=timezone.utc)}")
    print(f"🔍 Action source: {test_event['action_source']}")
    print(f"🔍 Has event_source_url: {'event_source_url' in test_event}")
    print(f"🔍 Event ID: {test_event['event_id']}")
    
    try:
        resp = requests.post(
            api_url,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=30
        )
        
        print(f"\n📊 Response Status: {resp.status_code}")
        print(f"📊 Response Headers: {dict(resp.headers)}")
        
        if resp.status_code == 200:
            response_data = resp.json()
            print("✅ Test event sent successfully!")
            print(f"📊 Response: {json.dumps(response_data, indent=2)}")
            
            # Save test results
            test_results = {
                "timestamp": datetime.now().isoformat(),
                "dataset_id": dataset_id,
                "test_email": test_email,
                "event_id": test_event["event_id"],
                "action_source": test_event["action_source"],
                "has_event_source_url": "event_source_url" in test_event,
                "response_status": resp.status_code,
                "response_data": response_data
            }
            
            os.makedirs("logs", exist_ok=True)
            test_file = f"logs/meta_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(test_file, "w") as f:
                json.dump(test_results, f, indent=2)
            print(f"📊 Test results saved to: {test_file}")
            
            print(f"\n🔧 NEXT STEPS:")
            print(f"1. Check Meta Events Manager → PRRC OFFLINE ACTIVE → Test events")
            print(f"2. Look for event ID: {test_event['event_id']}")
            print(f"3. Verify action_source shows as 'offline'")
            print(f"4. Check that no fake domain appears in diagnostics")
            
            return 0
            
        else:
            print(f"❌ API Error {resp.status_code}")
            print(f"Response: {resp.text}")
            return 1
            
    except Exception as e:
        print(f"❌ Request failed: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
