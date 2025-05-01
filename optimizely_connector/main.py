from run_signup_to_optimizely import fetch_events_and_registrations, get_all_races
from sync_to_optimizely import run_sync

def run_all():
    print("=== Pulling RunSignUp Data ===")
    race_list = get_all_races()
    fetch_events_and_registrations(race_list)
    print("=== RunSignUp data pull complete ===\n")

    print("=== Syncing Data to Optimizely ===")
    run_sync()
    print("=== Data sync to Optimizely complete ===\n")

if __name__ == "__main__":
    run_all()
