# RICS Purchase Sync Testing and Deployment Plan

## Overview
This guide walks through testing with Samantha Sprankle, then doing a catch-up sync, and finally enabling daily runs.

## Step 1: Test with Samantha Sprankle

### Setup GitHub Secrets
Go to your GitHub repository → Settings → Secrets and variables → Actions, and set:

1. **RICS_TEST_MODE** = `true`
2. **RICS_TEST_EMAIL** = `samantha.sprankle@example.com` (or her actual email if you have it)
3. **RICS_TEST_NAME** = `Samantha Sprankle` (or just `Sprankle` - partial match works)
4. **DRY_RUN** = `false` (to actually send data to Optimizely)

### Run the Test
1. Go to Actions → "Run PRRC Connector"
2. Click "Run workflow"
3. Select sync_type: **"daily"** (1 day lookback is fine for testing recent purchases)
4. Click "Run workflow"

### Verify Results
1. Check the workflow logs to see if Samantha's purchases were found and processed
2. Check Optimizely to verify:
   - Purchase events show up as "purchase" type (not "other")
   - Customer profile is updated correctly
   - Name appears correctly in Optimizely

### Iterate if Needed
If the name or data doesn't look right:
- Adjust `RICS_TEST_NAME` if needed (supports partial matching, case-insensitive)
- Check the workflow logs for any errors
- Re-run the workflow

## Step 2: Catch-Up Sync (Once Testing Confirms It Works)

### Update GitHub Secrets
1. **RICS_TEST_MODE** = `false` (disable test mode)
2. **DRY_RUN** = `false` (ensure it's off for production)

### Run Initial/Catch-Up Sync
1. Go to Actions → "Run PRRC Connector"
2. Click "Run workflow"
3. Select sync_type: **"initial"** (45 days lookback)
4. Click "Run workflow"

This will:
- Fetch all RICS purchases from the last 45 days
- Sync them to Optimizely as "purchase" events
- Update customer profiles
- Subscribe customers to the RICS list

### Monitor the Run
- The workflow will take longer (45 days of data)
- Check logs for any errors
- Verify in Optimizely that purchase events are coming through correctly

## Step 3: Enable Daily Runs

### Verify Scheduled Run is Active
The workflow already has a scheduled run configured:
- **Schedule**: Daily at 9:00 AM UTC (4:00 AM EST)
- **Sync Type**: Automatically uses "daily" (1 day lookback)

### No Action Needed
The daily runs will:
- Automatically fetch yesterday's purchases
- Sync to Optimizely
- Only process new purchases (deduplication prevents duplicates)

### Optional: Test Daily Run Manually
1. Go to Actions → "Run PRRC Connector"
2. Click "Run workflow"
3. Select sync_type: **"daily"**
4. Click "Run workflow"

## Summary of GitHub Secrets

| Secret | Step 1 (Test) | Step 2 (Catch-Up) | Step 3 (Daily) |
|--------|---------------|-------------------|----------------|
| `RICS_TEST_MODE` | `true` | `false` | `false` |
| `RICS_TEST_EMAIL` | Samantha's email | (not needed) | (not needed) |
| `RICS_TEST_NAME` | `Samantha Sprankle` | (not needed) | (not needed) |
| `DRY_RUN` | `false` | `false` | `false` |
| `RICS_LOOKBACK_DAYS` | (auto: 1) | (auto: 45) | (auto: 1) |

## Troubleshooting

### No purchases found for Samantha
- Check that `RICS_TEST_NAME` matches the name in RICS (case-insensitive, partial match)
- Verify the CSV file has recent data
- Check workflow logs for filtering messages

### Purchases still showing as "other" in Optimizely
- Verify the event type change was deployed (should be "purchase" not "rics_purchase")
- Check Optimizely event logs to see what event type is being sent

### Duplicate events
- The deduplication system should prevent this
- Check `logs/processed_rics_events.json` to see what's been processed
- If needed, you can clear this file to reprocess (but be careful!)

## Notes

- **Name Filtering**: The `RICS_TEST_NAME` supports partial, case-insensitive matching. So "Sprankle" will match "Samantha Sprankle", "sprankle", etc.
- **Test Mode Limits**: Test mode only processes the first 5 matching rows to keep testing fast
- **Deduplication**: Both ticket-level and event-level deduplication prevent duplicate events
- **Daily Runs**: Once enabled, daily runs happen automatically - no manual intervention needed

