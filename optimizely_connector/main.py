import os
import requests
from optimizely_connector.run_signup_to_optimizely import fetch_runsignup_data, fetch_events_and_registrations
from optimizely_connector.sync_rics_to_optimizely import run_sync
from dotenv import load_dotenv

load_dotenv()

def run_all():
    print("=== Starting RunSignUp to Optimizely Sync ===")

    # Debug environment values (DO NOT commit logs with sensitive info)
    print(f"API_KEY present: {bool(os.getenv('API_KEY'))}")
    print(f"API_SECRET present: {bool(os.getenv('API_SECRET'))}")
    print(f"PARTNER_ID: {os.getenv('PARTNER_ID')}")

    # Ensure data directory exists
    try:
        os.makedirs("data", exist_ok=True)
    except Exception as e:
        print(f"❌ Failed to create data directory: {e}")
        return

    print("\n=== Pulling RunSignUp Data ===")
    try:
        fetch_events_and_registrations()
        fetch_runsignup_data()
        print("✅ RunSignUp data pull complete\n")
    except Exception as e:
        print(f"❌ Error during RunSignUp data pull: {e}")
        return

    print("=== Syncing Data to Optimizely ===")
    try:
        run_sync()
        print("✅ Data sync to Optimizely complete\n")
    except Exception as e:
        print(f"❌ Error syncing to Optimizely: {e}")

if __name__ == "__main__":
    try:
        run_all()
    except Exception as e:
        print(f"🚨 Unhandled exception: {e}")
