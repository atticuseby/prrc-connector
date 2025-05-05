import os
from fetch_rics_data import fetch_rics_data
from dotenv import load_dotenv

load_dotenv()

def run_all():
    print("=== Pulling RICS Data ===")

    if not os.getenv("OPTIMIZELY_API_TOKEN"):
        print("❌ Missing OPTIMIZELY_API_TOKEN (used for RICS too)")
        return

    try:
        fetch_rics_data()
        print("✅ RICS data pull complete\n")
    except Exception as e:
        print(f"❌ Error during RICS data pull: {e}")
        return

if __name__ == "__main__":
    try:
        run_all()
    except Exception as e:
        print(f"🚨 Unhandled exception: {e}")
