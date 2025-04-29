import os
from dotenv import load_dotenv

load_dotenv()

OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"  # ← THIS LINE MATTERS
