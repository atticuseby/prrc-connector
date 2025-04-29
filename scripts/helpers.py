# Helper functions
import os
from datetime import datetime

def log_message(message, logfile="logs/sync_log.txt"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile, "a") as f:
        f.write(f"[{timestamp}] {message}\n")
