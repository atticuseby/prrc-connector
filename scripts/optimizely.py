# scripts/optimizely.py

import os
import requests

OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN", "").strip()
ODP_API_URL = "https://api.customer.io/v1/customers"

def send_to_optimizely(payload):
    if not OPTIMIZELY_API_TOKEN:
        print("❌ Missing OPTIMIZELY_API_TOKEN — cannot send to Optimizely")
        return

    headers = {
        "Authorization": f"Bearer {OPTIMIZELY_API_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(ODP_API_URL, json=payload, headers=headers)
        if response.status_code == 202:
            print(f"✅ [Optimizely] Accepted: {payload['identifiers'].get('email')}")
        else:
            print(f"❌ [Optimizely] Failed ({response.status_code}): {response.text}")
    except Exception as e:
        print(f"❌ [Optimizely] Exception: {e}")
