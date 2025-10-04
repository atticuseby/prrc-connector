# PRRC Connector Debugging & Optimization Plan

## 🔍 **ROOT CAUSE ANALYSIS**

Based on the logs and code analysis, I've identified the primary issues:

### 1. **RICS API Token Issue** ❌
- **Problem**: 401 Unauthorized errors in logs
- **Evidence**: `logs/sync_log.txt` shows repeated 401 errors
- **Impact**: No data is being fetched from RICS API
- **Result**: Empty CSV files (only headers, 206 bytes)

### 2. **Missing Diagnostic Visibility** ⚠️
- **Problem**: No counters to track where data is lost
- **Impact**: Can't identify if issue is API, filtering, or deduplication
- **Result**: Blind debugging

### 3. **Meta Offline Conversions Not Populating** ❌
- **Problem**: Events reach Event Manager but Ads Manager shows no conversions
- **Possible Causes**: Invalid data format, missing match keys, dataset attachment issues

## 🚀 **IMMEDIATE FIXES IMPLEMENTED**

### 1. **Enhanced `sync_rics_live.py`**
- ✅ Added comprehensive debug counters
- ✅ Added `--no-dedup` flag for testing
- ✅ Added API connection test before full sync
- ✅ Added detailed error reporting
- ✅ Added file analysis and validation

### 2. **RICS Token Diagnostic Tool**
- ✅ Created `scripts/diagnose_rics_token.py`
- ✅ Tests API connection with detailed error reporting
- ✅ Tests multiple store codes
- ✅ Provides clear action steps for token issues

### 3. **Meta Offline Events Test Tool**
- ✅ Created `scripts/test_meta_offline_events.py`
- ✅ Sends test event to validate integration
- ✅ Tests dataset info and offline conversions
- ✅ Provides troubleshooting guidance

### 4. **Debug Workflow**
- ✅ Created `.github/workflows/debug_connector.yml`
- ✅ Runs all diagnostic steps
- ✅ Uploads debug logs as artifacts
- ✅ Tests both with and without deduplication

## 🔧 **IMMEDIATE ACTION STEPS**

### Step 1: Fix RICS API Token
```bash
# Run locally to test token
python scripts/diagnose_rics_token.py
```

**If token is invalid:**
1. Log into RICS Enterprise: https://enterprise.ricssoftware.com
2. Go to Settings > API Keys
3. Generate a new API token
4. Update `RICS_API_TOKEN` secret in GitHub

### Step 2: Test Meta Integration
```bash
# Run locally to test Meta
python scripts/test_meta_offline_events.py
```

**If Meta test fails:**
1. Check `META_ACCESS_TOKEN` is valid
2. Check `META_DATASET_ID` is correct
3. Verify token has `offline_events` permission
4. Check dataset is not deleted/disabled

### Step 3: Run Debug Workflow
1. Go to GitHub Actions
2. Run "Debug PRRC Connector" workflow
3. Check the artifacts for debug logs
4. Analyze the counters to see where data is lost

### Step 4: Compare With/Without Dedup
```bash
# Test with deduplication
python scripts/sync_rics_live.py --debug

# Test without deduplication
python scripts/sync_rics_live.py --no-dedup --debug
```

## 📊 **DEBUG COUNTERS EXPLANATION**

The enhanced sync script now tracks:

- **`raw_count`**: Total rows fetched from RICS API
- **`after_cutoff_count`**: Rows after date filtering
- **`after_dedup_count`**: Rows after deduplication
- **`api_errors`**: Number of API errors encountered
- **`empty_responses`**: Number of empty API responses

## 🔍 **TROUBLESHOOTING GUIDE**

### If `raw_count = 0`:
- RICS API token is invalid/expired
- No transactions in last 7 days
- API endpoint changed
- Store codes are wrong

### If `raw_count > 0` but `after_cutoff_count = 0`:
- Date filter is too restrictive
- Wrong date field being used (SaleDateTime vs TicketDateTime)
- Timezone issues

### If `after_cutoff_count > 0` but `after_dedup_count = 0`:
- Deduplication is too aggressive
- All transactions are being marked as already sent
- Dedup logic has bugs

### If Meta events not showing in Ads Manager:
- Dataset not properly attached to campaigns
- Match keys are invalid/empty
- Event format doesn't match Meta requirements
- Processing delay (wait 15-30 minutes)

## 🎯 **EXPECTED OUTCOMES**

After implementing these fixes:

1. **Clear visibility** into where data is lost
2. **Working RICS API** connection with valid token
3. **Successful Meta integration** with test events
4. **Data flowing** through the entire pipeline
5. **Offline conversions** appearing in Ads Manager

## 📁 **FILES CREATED/MODIFIED**

### New Files:
- `scripts/diagnose_rics_token.py` - RICS API token diagnostic
- `scripts/test_meta_offline_events.py` - Meta integration test
- `.github/workflows/debug_connector.yml` - Debug workflow
- `test_local_debug.py` - Local testing script
- `DEBUGGING_PLAN.md` - This document

### Modified Files:
- `scripts/sync_rics_live.py` - Enhanced with debug counters
- `rics_connector/fetch_rics_data.py` - Added no-dedup support
- `.github/workflows/run_connector.yml` - Added debug flag

## 🚨 **CRITICAL NEXT STEPS**

1. **IMMEDIATE**: Fix RICS API token (this is blocking everything)
2. **IMMEDIATE**: Test Meta integration
3. **IMMEDIATE**: Run debug workflow to get baseline data
4. **FOLLOW-UP**: Analyze counters to identify remaining issues
5. **FOLLOW-UP**: Test with and without deduplication

The connector should be working within 30 minutes of fixing the RICS token!
