import os
import requests
from run_signup_to_optimizely import fetch_runsignup_data, fetch_events_and_registrations
from sync_rics_to_optimizely import run_sync
from extract_event_ids import extract_event_ids
from dotenv import load_dotenv

load_dotenv()

def run_all():
    print("=== Starting Full Connector Sync ===")
    print(f"API_KEY present: {bool(os.getenv('API_KEY'))}")
    print(f"API_SECRET present: {bool(os.getenv('API_SECRET'))}")
    print(f"OPTIMIZELY_API_TOKEN present: {bool(os.getenv('OPTIMIZELY_API_TOKEN'))}\n")

    print("=== Pulling RunSignUp Events & Registrations ===")
    try:
        fetch_events_and_registrations()
        fetch_runsignup_data()
        print("✅ RunSignUp sync complete\n")
    except Exception as e:
        print(f"❌ RunSignUp sync error: {e}")

    print("=== Extracting Event IDs from RunSignUp ===")
    try:
        extract_event_ids()
        print("✅ Event ID extraction complete\n")
    except Exception as e:
        print(f"❌ Error extracting event IDs: {e}")

    print("=== Pulling RICS Customers ===")
    try:
        from fetch_rics_data import fetch_rics_data
        fetch_rics_data()
    except Exception as e:
        print(f"❌ RICS data pull error: {e}")

    print("=== Syncing to Optimizely ===")
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
