import os
from run_signup_to_optimizely import fetch_runsignup_data, fetch_events_and_registrations
from sync_rics_to_optimizely import run_sync
from fetch_rics_data import fetch_rics_data
from dotenv import load_dotenv

load_dotenv()

def run_all():
    print("=== Starting Full Connector Sync ===")

    # Confirm environment vars
    print(f"API_KEY present: {bool(os.getenv('API_KEY'))}")
    print(f"API_SECRET present: {bool(os.getenv('API_SECRET'))}")
    print(f"OPTIMIZELY_API_TOKEN present: {bool(os.getenv('OPTIMIZELY_API_TOKEN'))}")

    try:
        os.makedirs("data", exist_ok=True)
    except Exception as e:
        print(f"❌ Failed to create data directory: {e}")
        return

    print("\n=== Pulling RunSignUp Events & Registrations ===")
    try:
        fetch_events_and_registrations()
        fetch_runsignup_data()
        print("✅ RunSignUp sync complete\n")
    except Exception as e:
        print(f"❌ RunSignUp sync error: {e}")
        return

    print("=== Pulling RICS Customers ===")
    try:
        fetch_rics_data()
        print("✅ RICS data pull complete\n")
    except Exception as e:
        print(f"❌ RICS data pull error: {e}")
        return

    print("=== Syncing to Optimizely ===")
    try:
        run_sync()
        print("✅ All data synced to Optimizely\n")
    except Exception as e:
        print(f"❌ Optimizely sync error: {e}")

if __name__ == "__main__":
    try:
        run_all()
    except Exception as e:
        print(f"🚨 Unhandled exception: {e}")
