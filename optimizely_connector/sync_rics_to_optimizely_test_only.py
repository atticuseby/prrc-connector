# optimizely_connector/sync_rics_to_optimizely_test_only.py

import sys
import os
import requests
import json

# 🛠 Add parent directory to import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.config import OPTIMIZELY_API_TOKEN

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/events"

def run_single_test_payload():
    print("🧪 [START] Beginning single profile test sync...")

    email = "odp_test_2025_001@banditmediagroup.com"
    user_id = "ODP_TEST_001"

    # ✅ Construct payload
    payload = [
        {
            "type": "customer_update",
            "identifiers": {
                "email": email,
                "user_id": user_id
            },
            "properties": {
                "first_name": "Test",
                "last_name": "User",
                "name": "Test User",
                "city": "Nashville",
                "state": "TN",
                "zip": "37201",
                "rics_id": "RICS-ODP-001",
                "orders": "3",
                "total_spent": "123.45"
            }
        }
    ]

    print("📦 [PAYLOAD] Prepared payload:")
    print(json.dumps(payload, indent=2))

    headers = {
        "x-api-key": OPTIMIZELY_API_TOKEN,
        "Content-Type": "application/json"
    }

    print("🧾 [HEADERS] Sending with headers:")
    print(json.dumps(headers, indent=2))

    try:
        print("🛰️ [REQUEST] Sending POST request to Optimizely...")
        response = requests.post(
            OPTIMIZELY_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=10
        )

        print("🔁 [RESPONSE STATUS] HTTP", response.status_code)
        print("📨 [RESPONSE BODY]")
        print(response.text)

        if response.status_code == 202:
            print("✅ [SUCCESS] 202 Accepted — Profile should now appear in ODP")
            exit(0)
        else:
            print("❌ [FAILURE] Unexpected status code — investigate above.")
            exit(1)

    except requests.exceptions.RequestException as e:
        print("🚨 [NETWORK ERROR] Request failed with exception:")
        print(e)
        exit(1)

if __name__ == "__main__":
    run_single_test_payload()
