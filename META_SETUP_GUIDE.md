# 🚀 Meta Sync Setup Guide - GET RUNNING ASAP

## The Problem
Your RICS to Meta sync is failing with a 400 Bad Request error. This is typically caused by:
- Invalid Meta credentials
- Wrong Offline Event Set ID
- Incorrect data format
- Missing permissions

## 🔧 Quick Fix Steps

### 1. Get Your Meta Credentials

**You need these from Meta Business Manager:**

1. **Offline Event Set ID** - Go to Events Manager → Offline Event Sets → Copy the ID
2. **Access Token** - Go to Business Settings → System Users → Generate token with `ads_management` permission

### 2. Update GitHub Secrets

Go to your GitHub repository → Settings → Secrets and variables → Actions

Add/update these secrets:
- `META_OFFLINE_SET_ID` - Your offline event set ID
- `META_OFFLINE_TOKEN` - Your access token with ads_management permission

### 3. Test Locally (Optional)

If you want to test before running the workflow:

```bash
# Set environment variables
export META_OFFLINE_SET_ID="your_set_id_here"
export META_OFFLINE_TOKEN="your_token_here"
export RICS_CSV_PATH="./data/rics.csv"

# Run diagnostics
python scripts/debug_meta_sync.py

# If diagnostics pass, run the sync
python scripts/sync_rics_to_meta.py
```

### 4. Run the Workflow

1. Go to your GitHub repository
2. Click "Actions" tab
3. Select "Sync RICS → Meta Offline Events"
4. Click "Run workflow"

## 🔍 What I Fixed

1. **Better Error Handling** - The script now shows detailed error messages
2. **Data Validation** - Validates CSV format and data before sending
3. **Connection Testing** - Tests Meta API access before attempting sync
4. **Smaller Batches** - Reduced batch size from 50 to 25 for better error handling
5. **Diagnostic Script** - New `debug_meta_sync.py` to identify issues

## 🚨 Common Issues & Solutions

### Error Code 100: Permission Denied
**Solution:** Add `ads_management` permission to your access token

### Error Code 190: Invalid Access Token
**Solution:** Generate a new access token in Meta Business Manager

### Error Code 294: Invalid Offline Event Set
**Solution:** Check that your Offline Event Set ID is correct

### 400 Bad Request: Data Format
**Solution:** The script now validates data format automatically

## 📞 Need Help?

If you're still having issues:

1. **Run the diagnostic script** - It will tell you exactly what's wrong
2. **Check the workflow logs** - Look for specific error messages
3. **Verify your credentials** - Make sure they're correct in GitHub secrets

## 🎯 Expected Output

When working correctly, you should see:
```
✅ Environment validation passed
✅ Offline Event Set found: [Your Set Name]
✅ Test event successful!
✅ Loaded 1,234 valid events from 1,250 CSV rows
🎉 All events processed successfully!
```

## ⚡ Quick Test

To test if your credentials work:

```bash
curl -X GET "https://graph.facebook.com/v16.0/YOUR_OFFLINE_SET_ID?access_token=YOUR_TOKEN"
```

If this returns JSON with your offline event set details, your credentials are correct. 