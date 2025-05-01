import requests
from run_signup_to_optimizely import fetch_runsignup_data, fetch_events_and_registrations
from sync_to_optimizely import run_sync

def run_all():
    print("=== Pulling RunSignUp Data ===")
    fetch_events_and_registrations()
    fetch_runsignup_data()
    print("=== RunSignUp data pull complete ===\n")

    print("=== Syncing Data to Optimizely ===")
    run_sync()
    print("=== Data sync to Optimizely complete ===\n")

if __name__ == "__main__":
    run_all()
