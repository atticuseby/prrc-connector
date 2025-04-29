import requests
from run_signup_to_optimizely import fetch_runsignup_data
from sync_to_optimizely import run_sync

# Optional: Add your heartbeat URL here
HEARTBEAT_URL = "https://uptime.betterstack.com/api/v1/heartbeat/jyo2DcPSxanN9hRwa3E1xo7z"

def run_all():
    print("=== Pulling RunSignUp Data ===")
    fetch_runsignup_data()
    print("=== RunSignUp data pull complete ===\n")

    print("=== Syncing Data to Optimizely ===")
    run_sync()
    print("=== Data sync to Optimizely complete ===\n")

    # Ping uptime monitor
    try:
        response = requests.get(HEARTBEAT_URL)
        print(f"Heartbeat sent. Status Code: {response.status_code}")
    except Exception as e:
        print(f"Failed to send heartbeat: {e}")

if __name__ == "__main__":
    run_all()
