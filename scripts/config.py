# scripts/config.py

import os

# ✅ Events API Token (for /v3/events only)
OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN", "")

# ✅ RICS Integration
RICS_API_TOKEN = os.getenv("RICS_API_TOKEN", "")

# ✅ Control Flag
DRY_RUN = os.getenv("DRY_RUN", "True").lower() == "true"

# ✅ Test Email
TEST_EMAIL = os.getenv("TEST_EMAIL", "test_email@yourdomain.com")

# ✅ OAuth Credentials for Profiles API (for /v3/profiles)
ODP_CLIENT_ID = os.getenv("ODP_CLIENT_ID", "")
ODP_CLIENT_SECRET = os.getenv("ODP_CLIENT_SECRET", "")
