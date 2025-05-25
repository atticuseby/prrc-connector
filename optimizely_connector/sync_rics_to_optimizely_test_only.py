# optimizely_connector/sync_rics_to_optimizely_test_only.py

import sys
import os
import requests
import json

# ✅ Ensure we can import from scripts/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.config import OPTIMIZELY_API_TOKEN

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/events"

def run_single_test_payload():
    print("🧪 Starting test payload sync...")

    payload = [
        {
            "type": "customer_update",
            "identifiers": {
                "email": "atticus@banditmediagroup.com"
            },
            "properties": {
                "first_name": "Test",
                "last_name": "User",
                "name": "Test User",
                "city": "Nashville",
                "state": "TN",
                "zip": "37201",
                "rics_id": "TEST-001",
                "orders": "3",
                "total_spent": "123.45"
            }
        }
    ]

    try:
        response = requests.post(
            OPTIMIZELY_ENDPOINT,
            headers={
                "x-api-key": OPTIMIZELY_API_TOKEN,
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=10
        )

        print("📨 Sent single test payload")
        print(f"🔁 Status: {response.status_code}")
        print(f"📝 Response: {response.text}")

        if response.status_code == 202:
            print("✅ SUCCESS: 202 Accepted — Profile should now appear in ODP")
            exit(0)
        else:
            print("❌ FAILED: Unexpected response — Check status and body above")
            exit(1)

    except requests.exceptions.RequestException as e:
        print(f"❌ Network error: {e}")
        exit(1)

if __name__ == "__main__":
    run_single_test_payload()
