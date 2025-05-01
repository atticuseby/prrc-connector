import requests
from run_signup_to_optimizely import fetch_runsignup_data, fetch_events_and_registrations
from sync_to_optimizely import run_sync

# Optional: Heartbeat URL (skip if not needed)
HEARTBEAT_URL = None  # Set to None or your actual heartbeat URL

def run_all():
    print("=== Pulling RunSignUp Data ===")
    fetch_events_and_registrations()  # ✅ NO ARGUMENTS
    fetch_runsignup_data()
    print("=== RunSignUp data pull complete ===\n")

    print("=== Syncing Data to Optimizely ===")
    run_sync()
    print("=== Data sync to Optimizely complete ===\n")

    if HEARTBEAT_URL:
        try:
            response = requests.get(HEARTBEAT_URL)
            print(f"Heartbeat sent. Status Code: {response.status_code}")
        except Exception as e:
            print(f"Failed to send heartbeat: {e}")

if __name__ == "__main__":
    run_all()
