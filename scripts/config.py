import os

# ✅ Support both local .env use and GitHub Actions secrets
OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN")

# ✅ Fallback: raise error if token is missing (so you catch it fast)
if not OPTIMIZELY_API_TOKEN:
    raise ValueError("❌ OPTIMIZELY_API_TOKEN is not set. Check .env or GitHub secrets.")

DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
