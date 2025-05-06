import os
import requests
from dotenv import load_dotenv

from run_signup_to_optimizely import fetch_runsignup_data, fetch_events_and_registrations
from sync_rics_to_optimizely import run_sync
from extract_event_ids import extract_event_ids
from fetch_rics_data import fetch_rics_data

load_dotenv()

def run_all():
    print("=== Starting Full Connector Sync ===")
    print(f"API_KEY present: {bool(os.getenv('API_KEY'))}")
    print(f"API_SECRET present: {bool(os.getenv('API_SECRET'))}")
    print(f"OPTIMIZELY_API_TOKEN present: {bool(os.getenv('OPTIMIZELY_API_TOKEN'))}\n")

    # RunSignUp Sync (test mode for now)
    print("=== Pulling RunSignUp Events & Registrations ===")
    print("(Skipping fetch_events_and_registrations – test mode enabled)")
    print("📥 Testing known race: Soldier Run (ID: 173466)")
    print("ℹ️ No registrations found (test mode)\n")

    # Event ID Extraction
    print("=== Extracting Event IDs from RunSignUp ===")
    try:
        extract_event_ids()
        print()
    except Exception as e:
        print(f"❌ Error extracting event IDs: {e}\n")

    # RICS Data Pull
    print("=== Pulling RICS Customers ===")
    try:
        fetch_rics_data()
    except Exception as e:
        print(f"❌ RICS data pull error: {e}\n")

    # Optimizely Sync
    print("=== Syncing to Optimizely ===")
    try:
        run_sync()
        print("✅ Optimizely sync complete\n")
    except Exception as e:
        print(f"❌ Error syncing to Optimizely: {e}\n")

if __name__ == "__main__":
    try:
        run_all()
    except Exception as e:
        print(f"🚨 Unhandled exception: {e}")
