# sync_rics_to_optimizely_test_only.py

import requests
import json
from scripts.config import OPTIMIZELY_API_TOKEN
from scripts.helpers import log_message

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/events"

def run_single_test_payload():
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

        log_message("📨 Sent single test payload")
        log_message(f"🔁 Status: {response.status_code}")
        log_message(f"📝 Response: {response.text}")
        if response.status_code == 202:
            log_message("✅ SUCCESS: 202 Accepted — Profile should now appear in ODP")
        else:
            log_message("❌ FAILED: Unexpected response — Check status and body above")

    except requests.exceptions.RequestException as e:
        log_message(f"❌ Network error: {e}")

if __name__ == "__main__":
    run_single_test_payload()
