# optimizely_connector/sync_rics_to_optimizely_test_only.py

import sys
import os
import requests
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.config import OPTIMIZELY_API_TOKEN

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/profiles"

def run_single_test_payload():
    print("🧪 [START] Sending profile to /v3/profiles...")

    payload = {
        "identifiers": {
            "email": "odp_test_minimal_2025@banditmediagroup.com"
        },
        "attributes": {
            "first_name": "Minimal",
            "last_name": "Test"
        }
    }

    print("📦 [PAYLOAD]")
    print(json.dumps(payload, indent=2))

    headers = {
        "x-api-key": OPTIMIZELY_API_TOKEN,
        "Content-Type": "application/json"
    }

    try:
        print("🛰️ [REQUEST] Sending POST...")
        response = requests.post(
            OPTIMIZELY_ENDPOINT,
            headers=headers,
            json=payload,
            timeout=10
        )

        print("🔁 [RESPONSE STATUS] HTTP", response.status_code)
        print("📨 [RESPONSE BODY]")
        print(response.text)

        if response.status_code in [200, 202]:
            print("✅ [SUCCESS] Profile created")
            exit(0)
        else:
            print("❌ [FAILURE] Status:", response.status_code)
            exit(1)

    except requests.exceptions.RequestException as e:
        print("🚨 [ERROR]", e)
        exit(1)

if __name__ == "__main__":
    run_single_test_payload()
