# RICS to Meta Workflow Debugging Guide

This guide helps you troubleshoot the RICS to Meta YAML workflow and `download_rics.py` script.

## 🔍 Quick Diagnosis

Run the test script to check your environment:

```bash
python3 test_env.py
```

This will tell you exactly what's missing.

## 🚨 Common Issues & Solutions

### 1. Missing Environment Variables

**Error:** `KeyError: 'GOOGLE_APPLICATION_CREDENTIALS'`

**Solution:** Set the required environment variables:

```bash
# For local testing, create a .env file:
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account.json"
export GDRIVE_FOLDER_ID_RICS="your_google_drive_folder_id"
export META_OFFLINE_SET_ID="your_meta_offline_event_set_id"
export META_OFFLINE_TOKEN="your_meta_access_token"
```

**For GitHub Actions:** Add these as repository secrets:
- `GDRIVE_SA_KEY` (base64 encoded service account JSON)
- `GDRIVE_FOLDER_ID_RICS`
- `META_OFFLINE_SET_ID`
- `META_OFFLINE_TOKEN`

### 2. Google Drive API Issues

**Error:** `No files found in the RICS Drive folder!`

**Solutions:**
- Check that `GDRIVE_FOLDER_ID_RICS` points to a valid Google Drive folder
- Ensure the service account has access to the folder
- Verify the folder contains CSV files
- Check that files aren't in the trash

**Error:** `403 Forbidden` or authentication errors

**Solutions:**
- Verify the service account JSON is valid
- Ensure the service account has the "Drive API" enabled
- Check that the service account has the correct permissions

### 3. CSV Format Issues

**Error:** `Missing required CSV fields`

**Required CSV columns:**
- `timestamp` (format: `YYYY-MM-DDTHH:MM:SS`)
- `order_id`
- `email`
- `phone`
- `total_amount`

**Test CSV format:**
```bash
python3 test_csv_format.py
```

### 4. Meta API Issues

**Error:** `Failed to send to Meta`

**Solutions:**
- Verify `META_OFFLINE_SET_ID` is correct
- Check that `META_OFFLINE_TOKEN` is valid and has the right permissions
- Ensure the offline event set is configured for the correct ad account
- Check Meta's API status page for any outages

## 🛠️ Step-by-Step Debugging

### Step 1: Test Environment
```bash
python3 test_env.py
```

### Step 2: Test Google Drive Access
```bash
# Set environment variables first
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/sa.json"
export GDRIVE_FOLDER_ID_RICS="your_folder_id"

python3 scripts/download_rics.py
```

### Step 3: Test CSV Format
```bash
python3 test_csv_format.py
```

### Step 4: Test Meta Sync (with small batch)
```bash
export META_OFFLINE_SET_ID="your_set_id"
export META_OFFLINE_TOKEN="your_token"
export BATCH_SIZE="5"  # Small batch for testing

python3 scripts/sync_rics_to_meta.py
```

## 📋 Required Setup Checklist

### Google Drive Setup
- [ ] Service account created in Google Cloud Console
- [ ] Drive API enabled
- [ ] Service account JSON downloaded
- [ ] Service account added to Google Drive folder with "Viewer" permissions
- [ ] Folder ID copied from Google Drive URL

### Meta Setup
- [ ] Facebook App created
- [ ] Offline Event Set created in Events Manager
- [ ] Access token generated with `ads_management` permission
- [ ] Offline Event Set ID copied

### Local Environment
- [ ] Python 3.x installed
- [ ] Dependencies installed: `pip install -r requirements.txt`
- [ ] Environment variables set
- [ ] Data directory exists: `mkdir -p data`

### GitHub Actions
- [ ] Repository secrets configured
- [ ] Service account JSON base64 encoded and added as `GDRIVE_SA_KEY`
- [ ] All other secrets added

## 🔧 Troubleshooting Commands

### Check Google Drive folder contents:
```python
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os

creds = service_account.Credentials.from_service_account_file(
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=creds)

folder_id = os.environ["GDRIVE_FOLDER_ID_RICS"]
resp = drive.files().list(
    q=f"'{folder_id}' in parents and trashed = false",
    fields="files(id,name,createdTime)"
).execute()

for file in resp.get("files", []):
    print(f"{file['name']} ({file['id']}) - {file['createdTime']}")
```

### Test Meta API connection:
```python
import requests

url = f"https://graph.facebook.com/v16.0/{os.environ['META_OFFLINE_SET_ID']}"
params = {"access_token": os.environ["META_OFFLINE_TOKEN"]}
resp = requests.get(url, params=params)
print(resp.json())
```

## 📞 Getting Help

If you're still having issues:

1. Check the logs in the `logs/` directory
2. Run the test scripts to identify specific problems
3. Verify all environment variables are set correctly
4. Test each component individually (Google Drive, CSV parsing, Meta API)
5. Check the GitHub Actions logs for detailed error messages

## 🔄 Workflow Steps

The complete workflow:

1. **Download RICS data** from Google Drive
2. **Parse CSV** and validate format
3. **Convert to Meta format** with hashed user data
4. **Upload to Meta** Offline Events API
5. **Log results** and handle errors

Each step can be tested independently using the provided test scripts. ne