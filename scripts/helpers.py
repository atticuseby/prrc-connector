# Helper functions
import os
from datetime import datetime
import sys

def log_message(msg):
    line = f"{datetime.now()} {msg}"
    print(line)  # ✅ ensures stdout capture
    sys.stdout.flush()
    try:
        with open("logs/fetch_rics_debug.log", "a") as f:
            f.write(line + "\n")
    except Exception:
        pass  # don’t crash if logs dir doesn’t exist
