# RunSignup → Optimizely Connector Fixes

## Summary

Fixed the `ModuleNotFoundError: No module named 'scripts.optimizely'` error and implemented a complete RunSignup → Optimizely pipeline that reads CSVs from Google Drive and posts to Optimizely.

## Files Changed

### 1. Created `runsignup_connector/optimizely_client.py` (NEW)

**Purpose**: Clean Optimizely API client with `post_profile()` and `post_event()` functions.

**Key Features**:
- Reads `OPTIMIZELY_API_TOKEN` from environment
- Uses `/v3/profiles` endpoint for profile updates
- Uses `/v3/events` endpoint for events
- Returns status codes and response text
- Raises helpful errors if token is missing

**Functions**:
- `post_profile(email: str, attrs: Dict) -> Tuple[int, str]`
- `post_event(email: str, event_name: str, properties: Dict, timestamp_iso: Optional[str]) -> Tuple[int, str]`

### 2. Rewrote `scripts/process_runsignup_csvs.py` (MAJOR UPDATE)

**Changes**:
- ✅ Removed broken import: `from scripts.optimizely import send_to_optimizely`
- ✅ Added import: `from runsignup_connector.optimizely_client import post_profile, post_event`
- ✅ Reads CSVs from Google Drive (not local directory)
- ✅ Maps RunSignup CSV headers to canonical keys:
  - `First Name` → `first_name`
  - `Last Name` → `last_name`
  - `Email Address` → `email`
  - `Event` → `event_name`
  - `Event Year` → `event_year`
  - `Registration Date` → `registration_ts` (converted to ISO 8601)
  - `Bib`, `Gender`, `Age`, `Race` → kept as optional event properties
- ✅ Email validation (skips invalid emails)
- ✅ DRY_RUN support (prints first 2 mapped rows, skips network calls)
- ✅ Comprehensive logging (row counts, success/failure)
- ✅ Respects `RSU_MAX_FILES` env var (default: 1, processes newest first)
- ✅ Posts both profile update and event per row
- ✅ Configurable event name via `OPTIMIZELY_EVENT_NAME` env var

**Header Mapping**:
```python
HEADER_MAP = {
    "First Name": "first_name",
    "Middle Name": "middle_name",
    "Last Name": "last_name",
    "Email Address": "email",
    "Event": "event_name",
    "Event Year": "event_year",
    "Registration Date": "registration_ts",
    "Bib": "bib",
    "Gender": "gender",
    "Age": "age",
    "Race": "race",
}
```

### 3. Updated `.github/workflows/runsignup_sync_to_optimizely.yml`

**Changes**:
- ✅ Added `PYTHONPATH` setup step
- ✅ Added sanity import step (fails fast if imports break)
- ✅ Added import test step (`tests/test_imports.py`)
- ✅ Changed run command to module form: `python -m runsignup_connector.main_runsignup`
- ✅ Added all required env vars:
  - `OPTIMIZELY_API_TOKEN`
  - `GDRIVE_CREDENTIALS`
  - `GDRIVE_FOLDER_ID`
  - `DRY_RUN`
  - `RSU_MAX_FILES` (default: '1')
  - `OPTIMIZELY_EVENT_NAME` (default: 'registration')

### 4. Created `tests/test_imports.py` (NEW)

**Purpose**: Sanity test that verifies all required modules can be imported.

**Tests**:
- `runsignup_connector.main_runsignup`
- `runsignup_connector.optimizely_client`
- `scripts.process_runsignup_csvs`
- `post_profile`, `post_event` functions

### 5. Created `runsignup_connector/__init__.py` (NEW)

Empty init file to make `runsignup_connector` a proper Python package.

### 6. Created `tests/__init__.py` (NEW)

Empty init file to make `tests` a proper Python package.

## Environment Variables Required

### GitHub Secrets (for Actions)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPTIMIZELY_API_TOKEN` | ✅ Yes | - | Optimizely API token |
| `GDRIVE_CREDENTIALS` | ✅ Yes | - | Google Drive service account JSON |
| `GDRIVE_FOLDER_ID` | ✅ Yes | - | Google Drive folder ID containing RunSignup CSVs |
| `DRY_RUN` | ⚠️ Recommended | `"true"` | Set to `"false"` to actually post data |
| `RSU_MAX_FILES` | ❌ Optional | `"1"` | Max number of CSV files to process (newest first) |
| `OPTIMIZELY_EVENT_NAME` | ❌ Optional | `"registration"` | Event name to use for Optimizely events |

## How to Test RunSignup → Optimizely

### Step 1: Set GitHub Secrets

1. Go to your repository → Settings → Secrets and variables → Actions
2. Ensure these secrets are set:
   - `OPTIMIZELY_API_TOKEN`
   - `GDRIVE_CREDENTIALS` (service account JSON as string)
   - `GDRIVE_FOLDER_ID` (folder ID containing RunSignup CSVs)
   - `DRY_RUN` = `"true"` (for testing)
   - `OPTIMIZELY_EVENT_NAME` = `"registration"` (or your preferred event name)

### Step 2: Test with DRY_RUN

1. Go to Actions → "Sync RunSignUp Data to Optimizely"
2. Click "Run workflow" → Run
3. Check the logs:
   - ✅ Sanity import should pass
   - ✅ Import tests should pass
   - ✅ Should show: "Files processed: X"
   - ✅ Should show: "Total rows: X, Valid rows: X, Skipped rows: X"
   - ✅ Should show sample mapped rows (first 2) with profile attrs and event props
   - ✅ Should show "DRY_RUN: True"
   - ✅ Should NOT make any network calls to Optimizely

### Step 3: Verify CSV Format

Ensure your Google Drive folder contains CSVs with these headers (case-sensitive):
- `First Name`
- `Middle Name` (optional)
- `Last Name`
- `Email Address`
- `Bib` (optional)
- `Gender` (optional)
- `Age` (optional)
- `Race` (optional)
- `Event`
- `Event Year`
- `Registration Date`

### Step 4: Test with Real Data (DRY_RUN=false)

1. Set `DRY_RUN` secret to `"false"`
2. Ensure you have a test CSV with 1-2 rows in the Google Drive folder
3. Run the workflow
4. Check Optimizely Activity:
   - Should see profile updates for test emails
   - Should see events with the configured event name
5. Verify the data matches what was in the CSV

### Step 5: Monitor Logs

The workflow logs will show:
- Files processed count
- Total/valid/skipped row counts
- Posted profiles/events counts
- Any errors with file names and row indices

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError`:
1. Check that `PYTHONPATH` is set in the workflow
2. Verify `runsignup_connector/__init__.py` exists
3. Check the sanity import step output

### Google Drive Errors

If you see "Failed to connect to Google Drive":
1. Verify `GDRIVE_CREDENTIALS` is valid JSON
2. Verify `GDRIVE_FOLDER_ID` is correct
3. Ensure service account has access to the folder

### Optimizely API Errors

If you see non-200 responses:
1. Verify `OPTIMIZELY_API_TOKEN` is valid
2. Check Optimizely API status
3. Review response text in logs for specific error messages

### No Files Found

If you see "No CSV files found":
1. Verify CSVs are in the correct Google Drive folder
2. Check that files are not in trash
3. Verify folder sharing permissions

## Code Structure

```
runsignup_connector/
├── __init__.py                    # Package init
├── main_runsignup.py              # Entry point (calls processor)
└── optimizely_client.py           # Optimizely API client (NEW)

scripts/
└── process_runsignup_csvs.py      # CSV processor (REWRITTEN)

tests/
├── __init__.py                    # Package init (NEW)
└── test_imports.py                # Import sanity test (NEW)

.github/workflows/
└── runsignup_sync_to_optimizely.yml  # GitHub Actions workflow (UPDATED)
```

## Next Steps

After testing, you'll need to answer these two questions:

1. **Which Optimizely event name should we use for registrations?** (exact case and spaces)
   - Currently defaults to `"registration"` but can be configured via `OPTIMIZELY_EVENT_NAME`

2. **Do we need to subscribe contacts to a specific Optimizely list on registration?**
   - If yes, provide the list ID or exact name to map

Once you provide these answers, I can update the code accordingly.

