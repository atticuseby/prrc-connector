import os

OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN")
RICS_API_TOKEN = os.getenv("RICS_API_TOKEN")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
