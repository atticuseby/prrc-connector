# sync_rics_to_optimizely_test_only.py

import sys
import os
import requests
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.config import ODP_CLIENT_ID, ODP_CLIENT_SECRET

OAUTH_URL = "https://api.zaius.com/oauth/access_token"
PROFILES_URL = "https://api.zaius.com/v3/profiles"


def get_access_token():
    print("🔐 [AUTH] Requesting access token...")
    try:
        response = requests.post(OAUTH_URL, data={
            "grant_type": "client_credentials",
            "client_id": ODP_CLIENT_ID,
            "client_secret": ODP_CLIENT_SECRET
        })

        if response.status_code != 200:
            print("❌ [AUTH FAILURE] Status:", response.status_code)
            print(response.text)
            exit(1)

        token = response.json()["access_token"]
        print("✅ [AUTH] Access token received")
        return token
    except Exception as e:
        print("🚨 [AUTH ERROR]", e)
        exit(1)


def run_single_test_payload(token):
    print("🧪 [START] Sending profile to /v3/profiles...")

    payload = {
        "identifiers": {
            "email": "odp_test_realtime_2025@banditmediagroup.com"
        },
        "attributes": {
            "first_name": "RealTime",
            "last_name": "OAuth",
            "name": "RealTime OAuth",
            "city": "Nashville",
            "state": "TN",
            "zip": "37201",
            "rics_id": "RICS-ODP-REALTIME-001",
            "orders": "5",
            "total_spent": "543.21"
        }
    }

    print("📦 [PAYLOAD]")
    print(json.dumps(payload, indent=2))

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    print("🧾 [HEADERS]")
    print(json.dumps(headers, indent=2))

    try:
        print("🛰️ [REQUEST] Sending POST to /v3/profiles...")
        response = requests.post(
            PROFILES_URL,
            headers=headers,
            json=payload,
            timeout=10
        )

        print("🔁 [RESPONSE STATUS] HTTP", response.status_code)
        print("📨 [RESPONSE BODY]")
        print(response.text)

        if response.status_code in [200, 202]:
            print("✅ [SUCCESS] Profile created immediately in ODP")
            exit(0)
        else:
            print("❌ [FAILURE] Unexpected status — check response above.")
            exit(1)

    except requests.exceptions.RequestException as e:
        print("🚨 [REQUEST ERROR]")
        print(e)
        exit(1)


if __name__ == "__main__":
    token = get_access_token()
    run_single_test_payload(token)
