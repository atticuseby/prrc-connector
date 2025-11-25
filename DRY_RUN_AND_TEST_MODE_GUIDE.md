# DRY_RUN and TEST_MODE Guide

## ⚠️ IMPORTANT: DRY_RUN Defaults to "true" for Safety

Both RunSignup and RICS syncs **default to DRY_RUN="true"** if the secret is not set. This means:
- ✅ **Safe by default** - won't post data unless explicitly enabled
- ✅ **No accidental data posting** - must set secret to "false" to actually run
- ✅ **RunSignup is currently safe** - it reads from `${{ secrets.DRY_RUN }}` which defaults to "true" if not set

## RunSignup DRY_RUN Status

### How to Check if RunSignup is Actually Running:

1. **Check GitHub Secrets:**
   - Go to: Repository → Settings → Secrets and variables → Actions
   - Look for `DRY_RUN` secret
   - If it's set to `"false"` → **It WILL post data**
   - If it's set to `"true"` or missing → **It will NOT post data (safe)**

2. **Check Workflow Logs:**
   - Look for: `DRY_RUN: True` or `DRY_RUN: False` in the logs
   - If `DRY_RUN: True` → Safe, no data posted
   - If `DRY_RUN: False` → **Actually posting data**

3. **Code Default:**
   ```python
   DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
   ```
   - Defaults to `"true"` if env var is not set
   - Only runs for real if `DRY_RUN="false"` is explicitly set

### Current RunSignup Status:
- ✅ **Safe** - The workflow uses `${{ secrets.DRY_RUN }}` which will be `"true"` by default
- ✅ **Won't break anything** - Even if running, it defaults to safe mode
- ✅ **Check the secret** - If you want it to actually run, set `DRY_RUN="false"` in GitHub Secrets

## Fast Testing with TEST_MODE

### RunSignup TEST_MODE:
```bash
RSU_TEST_MODE=true
RSU_TEST_EMAIL=your-test-email@example.com
```
- Processes only **5 rows** (very fast!)
- Overrides all emails with `RSU_TEST_EMAIL`
- Perfect for quick debugging

### RICS TEST_MODE:
```bash
RICS_TEST_MODE=true
RICS_TEST_EMAIL=your-test-email@example.com
```
- Processes only **5 rows** (very fast!)
- Overrides all emails with `RICS_TEST_EMAIL`
- Perfect for quick debugging

### How to Use TEST_MODE in GitHub Actions:

1. **Add to workflow env vars:**
   ```yaml
   env:
     RICS_TEST_MODE: ${{ secrets.RICS_TEST_MODE || 'false' }}
     RICS_TEST_EMAIL: ${{ secrets.RICS_TEST_EMAIL || '' }}
   ```

2. **Or set as GitHub Secrets:**
   - `RICS_TEST_MODE` = `"true"`
   - `RICS_TEST_EMAIL` = `"test@example.com"`

3. **Run the workflow** - it will process only 5 rows in seconds!

## Summary

| Mode | What It Does | Speed | Safe? |
|------|--------------|-------|-------|
| **DRY_RUN=true** | No API calls, just logs | Fast | ✅ Safe |
| **DRY_RUN=false + TEST_MODE** | Posts 5 rows to test email | Very Fast | ✅ Safe (test data) |
| **DRY_RUN=false** | Posts all data | Slow | ⚠️ Live data |

## Recommendations

1. **For RunSignup (currently running):**
   - ✅ It's safe - defaults to DRY_RUN=true
   - ✅ Check the secret if you want it to actually run
   - ✅ Use TEST_MODE for quick testing

2. **For RICS (testing):**
   - ✅ Use TEST_MODE for fast debugging (5 rows, ~10 seconds)
   - ✅ Use DRY_RUN=true for full validation (no API calls)
   - ✅ Use DRY_RUN=false only when ready for production

