import os
import sys

# 🔧 Fix import path for GitHub Actions
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.process_runsignup_csvs import process_runsignup_csvs

def run_all():
    print("=== RUNSIGNUP ➜ OPTIMIZELY SYNC START ===")
    rows_processed = process_runsignup_csvs()
    
    if rows_processed > 0:
        print(f"✅ {rows_processed} records pushed to Optimizely")
    else:
        print("ℹ️ No records processed (no CSV rows found after filtering).")

if __name__ == "__main__":
    run_all()
