# optimizely_connector/sync_rics_to_optimizely_test_only.py

import sys
import os
import requests
import json

# Add parent directory to import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.config import OPTIMIZELY_API_TOKEN

OPTIMIZELY_ENDPOINT = "https://api.zaius.com/v3/profiles"

def run_single_test_payload():
    print("🧪 [START] Sending profile via /v3/profiles...")

    email = "odp_test_2025_002@banditmediagroup.com"  # <- brand-new test email

    payload = [
        {
            "identifiers": {
                "email": email
            },
            "properties": {
                "first_name": "RealTime",
                "last_name": "ProfileTest",
                "name": "RealTime ProfileTest",
                "city": "Nashville",
                "state": "TN",
                "zip": "37201",
                "rics_id": "RICS-ODP-002",
                "orders": "5",
                "total_spent": "543.21"
            }
        }
    ]

    print("📦 [PAYLOAD]")
    print(json.dumps(payload, indent=2))

    headers = {
        "x-api-key": OPTIMIZELY_API_TOKEN,
        "Content-Type": "application/json"
    }

    print("🧾 [HEADERS]")
    print(json.dumps(headers, indent=2))

    try:
        print("🛰️ [REQUEST] Sending POST to /v3/profiles...")
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
            print("✅ [SUCCESS] Profile should now appear in ODP immediately")
            exit(0)
        else:
            print("❌ [FAILURE] Unexpected status — check response above.")
            exit(1)

    except requests.exceptions.RequestException as e:
        print("🚨 [NETWORK ERROR]")
        print(e)
        exit(1)

if __name__ == "__main__":
    run_single_test_payload()
