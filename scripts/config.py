# config.py

import os

OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN", "")
RICS_API_TOKEN = os.getenv("RICS_API_TOKEN", "")
DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"
TEST_EMAIL = os.getenv("TEST_EMAIL", "test_email@yourdomain.com")
