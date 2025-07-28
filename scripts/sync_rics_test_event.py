import requests
import time
import hashlib
import json

# === REQUIRED SETTINGS ===
ACCESS_TOKEN = 'EAAVHTfyS4JMBPMKeZCZBhxjoidZBZAw813eZCmTO1zLZARnL9tKJbWSaSK4zlgeFmgyEFzRlxoI9t4KuLK8K8ZAJTZBOKqfrXvd7QuthZBk8cJhfg0SnUKWWwfR7ObZC2AZBxf8jUJaUzsZB3ZBJrP1LgR8N5ZCFUgWZAl8XL1gk5xanqZAQ330ZBlfcm31jZARTnp632wbTZArQwZDZD'  # ← Replace this with your Meta token
EVENT_SET_ID = '855183627077424'        # ← Confirm this is your Offline Event Set ID
TEST_EVENT_CODE = 'TEST3026' # ← Paste the code from Meta Test Events tab

# === SAMPLE MATCH KEYS (hashed as required by Meta) ===
def hash_data(value):
    return hashlib.sha256(value.strip().lower().encode('utf-8')).hexdigest()

match_keys = {
    'em': hash_data('testuser@example.com'),
    'ph': hash_data('1234567890'),
    'fn': hash_data('Test'),
    'ln': hash_data('User'),
    'ct': hash_data('New York'),
    'st': hash_data('NY'),
    'zp': hash_data('10001'),
    'country': hash_data('us')
}

# === EVENT DATA ===
event = {
    "match_keys": match_keys,
    "event_name": "Purchase",
    "event_time": int(time.time()),
    "value": 99.99,
    "currency": "USD"
}

# === PAYLOAD WITH TEST CODE ===
payload = {
    "data": [event],
    "test_event_code": TEST_EVENT_CODE
}

# === SEND TO META ===
url = f"https://graph.facebook.com/v18.0/{EVENT_SET_ID}/events"
headers = {"Content-Type": "application/json"}
params = {"access_token": ACCESS_TOKEN}

response = requests.post(url, headers=headers, params=params, data=json.dumps(payload))

print("Status:", response.status_code)
print("Response:", response.json())
