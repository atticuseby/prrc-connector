# runsignup_connector/main_runsignup.py

from scripts.process_runsignup_csvs import process_runsignup_csvs

def run_all():
    print("=== RUNSIGNUP ➜ OPTIMIZELY SYNC START ===")
    process_runsignup_csvs()
    print("✅ All new records pushed to Optimizely")

if __name__ == "__main__":
    run_all()
