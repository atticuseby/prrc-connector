# RICS to Optimizely Sync - Current State Analysis

## Current Implementation

### 1. Data Fetching (`rics_connector/fetch_rics_data.py`)
- **API Endpoint**: `GetPOSTransaction` (`/api/POS/GetPOSTransaction`)
- **Method**: Fetches POS transactions from multiple stores (1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99)
- **Date Range**: Last 45 days
- **Output**: CSV file with purchase history (`rics_customer_purchase_history_*.csv`)
- **Deduplication**: Tracks `TicketNumber` in `logs/sent_ticket_ids.csv`
- **Fields Captured**:
  - TicketDateTime, TicketNumber, SaleDateTime
  - StoreCode, TerminalId, Cashier
  - CustomerId, CustomerName, CustomerEmail, CustomerPhone
  - Sku, Description, Quantity, AmountPaid, Discount, Department, SupplierName

### 2. Current Sync Scripts

#### A. `rics_connector/sync_rics_to_optimizely.py` (NOT USED IN WORKFLOW)
- Reads CSVs from `/data` folder
- Uses correct Optimizely endpoint: `https://api.zaius.com/v3/events`
- Sends `customer_update` events
- Batches events (500 per batch)
- Subscribes to `OPTIMIZELY_LIST_ID_RICS` list
- **Issue**: Not being used by the workflow

#### B. `scripts/sync_to_optimizely.py` (CURRENTLY USED IN WORKFLOW)
- Reads CSV from command line argument
- **WRONG ENDPOINT**: Uses `https://api.customer.io/v1/customers` (not Optimizely!)
- Sends individual requests (no batching)
- Only sends customer attributes, no purchase events
- **Issue**: This is completely wrong - it's not even Optimizely!

### 3. GitHub Workflow (`.github/workflows/run_connector.yml`)
```
1. sync_rics_live.py → Fetches RICS data, writes CSV
2. sync_to_optimizely.py → Sends to WRONG endpoint (customer.io)
3. sync_rics_to_meta.py → Sends to Meta
```

## Issues Identified

### Critical Issues:
1. **Wrong API Endpoint**: `scripts/sync_to_optimizely.py` uses `customer.io` instead of `zaius.com`
2. **No Purchase Events**: Only sends customer updates, not individual purchase events
3. **No Subscription Logic**: Doesn't use the proper subscription endpoint like RunSignup does
4. **Not Using Correct Script**: `rics_connector/sync_rics_to_optimizely.py` exists but isn't used

### Missing Features:
1. **Purchase Events**: Should post `rics_purchase` events for each transaction
2. **Subscription Logic**: Should use `upsert_profile_with_subscription` like RunSignup
3. **Event Deduplication**: Should track processed events to avoid duplicates
4. **Batch Processing**: Should batch events efficiently

## Recommended Solution

### Option 1: Fix Existing Script (Recommended)
- Update `rics_connector/sync_rics_to_optimizely.py` to:
  - Read from correct location (CSV from fetch step)
  - Post purchase events (not just customer updates)
  - Use `upsert_profile_with_subscription` for proper subscription handling
  - Add event deduplication
  - Update workflow to use this script

### Option 2: Use GetCustomerPurchaseHistory API
- Switch from `GetPOSTransaction` to `GetCustomerPurchaseHistory`
- This might be more appropriate for customer-level purchase history
- Would require refactoring the fetch logic

## Next Steps

1. ✅ Analyze current state (DONE)
2. ⏳ Fix `rics_connector/sync_rics_to_optimizely.py` to:
   - Post purchase events
   - Use proper subscription logic
   - Add deduplication
3. ⏳ Update workflow to use correct script
4. ⏳ Test end-to-end
5. ⏳ Verify all purchases are syncing

