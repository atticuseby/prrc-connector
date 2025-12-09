"""
Process RICS purchase history CSV and sync to Optimizely.

Reads purchase history CSV, posts purchase events, and updates customer profiles
with proper subscription handling (respects unsubscribe states).

DEDUPLICATION STRATEGY:
This script implements two-layer deduplication to prevent duplicate events in Optimizely:

1. **Ticket-level deduplication** (in fetch_rics_data.py):
   - Tracks ticket numbers in `logs/sent_ticket_ids.csv`
   - Prevents re-fetching the same tickets from RICS API
   - Works across both initial (45-day) and daily (1-day) syncs

2. **Event-level deduplication** (in this script):
   - Tracks event keys (SHA256 hash of email + ticket_number + sku + timestamp) in `logs/processed_rics_events.json`
   - Prevents posting duplicate purchase events to Optimizely
   - Checks deduplication EARLY (before any API calls) for efficiency
   - Works across both initial and daily syncs

This ensures that:
- Initial 45-day sync won't create duplicates when run multiple times
- Daily 1-day syncs won't re-process events from previous days
- Same purchase event (same email, ticket, SKU, timestamp) is only posted once to Optimizely
"""

import os
import sys
import json
import csv
import hashlib
from datetime import datetime, timezone
from typing import Dict, Optional, Set

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from runsignup_connector.optimizely_client import (
    upsert_profile_with_subscription,
    post_events_batch
)

# Configuration
# IMPORTANT: DRY_RUN defaults to "true" for safety
# Set DRY_RUN="false" in GitHub Secrets to actually post data
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
# List ID for "Base - Store Purchases - Automated" email list
# Can be overridden via OPTIMIZELY_LIST_ID_RICS env var, but defaults to base_store_purchases_only
OPTIMIZELY_LIST_ID_RICS = os.getenv("OPTIMIZELY_LIST_ID_RICS", "base_store_purchases_only").strip()
OPTIMIZELY_EVENT_NAME = "purchase"  # Event type for purchases (standard Optimizely purchase event)

# TEST MODE configuration - for fast debugging (processes only 5 rows)
RICS_TEST_MODE = os.getenv("RICS_TEST_MODE", "false").lower() == "true"
RICS_TEST_EMAIL = os.getenv("RICS_TEST_EMAIL", "").strip()
RICS_TEST_NAME = os.getenv("RICS_TEST_NAME", "").strip()  # Filter by customer name (case-insensitive partial match)
RICS_TEST_EMAIL_FILTER = os.getenv("RICS_TEST_EMAIL_FILTER", "").strip()  # Filter by customer email (more reliable than name)
RICS_TEST_MAX_ROWS = 5  # Process only 5 rows in test mode

# Event deduplication
PROCESSED_EVENTS_LOG = os.path.join(os.path.dirname(__file__), "..", "logs", "processed_rics_events.json")

# Batch configuration
EVENT_BATCH_SIZE = 100  # Batch events for efficiency


def _normalize_email(email: str) -> Optional[str]:
    """Normalize email address."""
    if not email:
        return None
    email = email.strip().lower()
    if "@" not in email:
        return None
    return email


def _generate_event_key(email: str, ticket_number: str, sku: str, purchase_ts: Optional[str]) -> str:
    """
    Generate a unique key for a purchase event to use for deduplication.
    
    Uses email + ticket_number + sku + timestamp to uniquely identify a purchase event.
    
    Args:
        email: Normalized email address
        ticket_number: RICS ticket number
        sku: Product SKU
        purchase_ts: Purchase timestamp (ISO format)
        
    Returns:
        SHA256 hash of the event key components
    """
    key_parts = [
        email.lower().strip() if email else "",
        str(ticket_number).strip(),
        str(sku).strip(),
        str(purchase_ts or "").strip()
    ]
    
    key_string = "|".join(key_parts)
    return hashlib.sha256(key_string.encode("utf-8")).hexdigest()


def load_processed_events() -> Set[str]:
    """
    Load set of previously processed event keys from log file.
    
    Returns:
        Set of event key hashes
    """
    if not os.path.exists(PROCESSED_EVENTS_LOG):
        return set()
    
    try:
        with open(PROCESSED_EVENTS_LOG, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return set(data)
            elif isinstance(data, dict) and "events" in data:
                return set(data["events"])
            else:
                return set()
    except Exception as e:
        print(f"⚠️ Warning: Could not load processed events log: {e}")
        return set()


def save_processed_events(event_keys: Set[str]):
    """
    Save processed event keys to log file.
    
    Args:
        event_keys: Set of event key hashes to save
    """
    os.makedirs(os.path.dirname(PROCESSED_EVENTS_LOG), exist_ok=True)
    
    data = {
        "events": sorted(list(event_keys)),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_events": len(event_keys)
    }
    
    try:
        with open(PROCESSED_EVENTS_LOG, "w") as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        print(f"⚠️ Warning: Could not save processed events log: {e}")


def _parse_timestamp(ts_str: str) -> Optional[str]:
    """Parse RICS timestamp and convert to ISO 8601 format."""
    if not ts_str:
        return None
    
    # Try common date formats from RICS
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(ts_str, fmt)
            # Assume UTC if no timezone info
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            continue
    
    return None


def process_rics_purchases(csv_path: str):
    """
    Main processing function: read RICS purchase CSV and sync to Optimizely.
    
    Args:
        csv_path: Path to the RICS purchase history CSV file
    """
    print("=== RICS PURCHASE SYNC TO OPTIMIZELY ===")
    print(f"DRY_RUN: {DRY_RUN}")
    print(f"TEST_MODE: {RICS_TEST_MODE}")
    if RICS_TEST_MODE:
        print(f"TEST_EMAIL: {RICS_TEST_EMAIL}")
        if RICS_TEST_EMAIL_FILTER:
            print(f"TEST_EMAIL_FILTER: {RICS_TEST_EMAIL_FILTER} (filtering by email)")
        elif RICS_TEST_NAME:
            print(f"TEST_NAME: {RICS_TEST_NAME} (filtering by name)")
        print(f"TEST_MAX_ROWS: {RICS_TEST_MAX_ROWS}")
        print("⚠️  TEST MODE: Only processing first 5 rows and overriding emails with TEST_EMAIL")
    print(f"CSV file: {csv_path}")
    print()
    
    # Validate required env vars
    if not os.getenv("OPTIMIZELY_API_TOKEN"):
        raise RuntimeError("Missing required env: OPTIMIZELY_API_TOKEN")
    
    if not OPTIMIZELY_LIST_ID_RICS:
        raise RuntimeError("OPTIMIZELY_LIST_ID_RICS must be set - required for subscribing store purchase customers")
    print(f"📋 Subscribing customers to list: {OPTIMIZELY_LIST_ID_RICS}")
    print("   (Respects existing unsubscribes - will not re-subscribe if previously unsubscribed)")
    print()
    
    # Check if CSV file exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Load processed events for deduplication
    processed_event_keys = load_processed_events()
    new_event_keys = set()
    skipped_duplicate_events = 0
    
    if not DRY_RUN:
        print(f"📋 Loaded {len(processed_event_keys)} previously processed events for deduplication")
    
    # Track stats
    total_rows = 0
    valid_rows = 0
    skipped_rows = 0
    skip_reasons = {
        "email_filter_no_match": 0,
        "name_filter_no_match": 0,
        "missing_email": 0,
        "duplicate_event": 0,
        "exception": 0
    }
    posted_profiles = 0
    posted_events = 0
    subscribed_to_lists = 0
    rows_processed = 0  # Track rows actually processed (for TEST_MODE)
    
    # Batch event collection
    event_batch = []
    event_batch_keys = []  # Track event keys for each batch (for deduplication after successful post)
    
    # TEST MODE validation
    if RICS_TEST_MODE:
        if not RICS_TEST_EMAIL:
            raise RuntimeError("RICS_TEST_MODE is true but RICS_TEST_EMAIL is not set")
        print(f"🧪 TEST MODE: Only processing first {RICS_TEST_MAX_ROWS} rows")
        print(f"🧪 TEST MODE: Overriding all emails with {RICS_TEST_EMAIL}")
        if RICS_TEST_EMAIL_FILTER:
            print(f"🧪 TEST MODE: Filtering by customer email matching '{RICS_TEST_EMAIL_FILTER}'")
        elif RICS_TEST_NAME:
            print(f"🧪 TEST MODE: Filtering by customer name containing '{RICS_TEST_NAME}'")
        print()
    
    # Process CSV
    print(f"📄 Processing CSV: {csv_path}")
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Check file size before processing
    file_size = os.path.getsize(csv_path)
    print(f"📊 CSV file: {os.path.basename(csv_path)} ({file_size:,} bytes)")
    
    with open(csv_path, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row_idx, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            # TEST MODE: Only process first 5 rows
            if RICS_TEST_MODE and rows_processed >= RICS_TEST_MAX_ROWS:
                print(f"\n⚠️  TEST MODE: Reached max rows ({RICS_TEST_MAX_ROWS}), stopping processing")
                break
            
            total_rows += 1
            
            # Progress logging every 100 rows
            if total_rows % 100 == 0:
                if not DRY_RUN:
                    print(f"⏳ Progress: Processed {total_rows} rows (valid: {valid_rows}, skipped: {skipped_rows})")
                    if posted_profiles > 0:
                        print(f"   Posted: {posted_profiles} profiles, {posted_events} events, {subscribed_to_lists} subscriptions")
                else:
                    print(f"⏳ Progress: [DRY_RUN] Processed {total_rows} rows (valid: {valid_rows}, skipped: {skipped_rows})")
            
            try:
                # Extract customer info
                original_email = _normalize_email(row.get("CustomerEmail", ""))
                phone = row.get("CustomerPhone", "").strip()
                customer_name = row.get("CustomerName", "").strip()
                
                # TEST MODE: Filter by email if specified (more reliable than name)
                if RICS_TEST_MODE and RICS_TEST_EMAIL_FILTER:
                    if not original_email or original_email.lower() != RICS_TEST_EMAIL_FILTER.lower():
                        skipped_rows += 1
                        skip_reasons["email_filter_no_match"] += 1
                        continue  # Skip rows that don't match the email filter
                    # If we get here, we found a match - log only first match
                    if rows_processed == 0:
                        print(f"✅ Found matching email: '{original_email}'")
                
                # TEST MODE: Filter by name if specified (only if email filter not set)
                elif RICS_TEST_MODE and RICS_TEST_NAME:
                    if not customer_name or RICS_TEST_NAME.lower() not in customer_name.lower():
                        skipped_rows += 1
                        skip_reasons["name_filter_no_match"] += 1
                        continue  # Skip rows that don't match the name filter
                    # If we get here, we found a match - log only first match
                    if rows_processed == 0:
                        print(f"✅ Found matching name: '{customer_name}'")
                
                # TEST MODE: Override email with test email
                if RICS_TEST_MODE:
                    if not original_email:
                        # Still need an email for TEST_MODE
                        original_email = RICS_TEST_EMAIL
                    email = RICS_TEST_EMAIL
                    # Only log first override
                    if rows_processed == 0:
                        print(f"🧪 TEST MODE: Using test email '{email}' for all rows")
                else:
                    email = original_email
                
                if not email:
                    skipped_rows += 1
                    skip_reasons["missing_email"] += 1
                    continue
                
                # Extract purchase info
                ticket_number = str(row.get("TicketNumber", "")).strip()
                ticket_datetime = row.get("TicketDateTime", "")
                purchase_ts = _parse_timestamp(ticket_datetime)
                
                # Build profile attributes
                profile_attrs = {
                    "first_name": row.get("CustomerName", "").split()[0] if row.get("CustomerName") else "",
                    "last_name": " ".join(row.get("CustomerName", "").split()[1:]) if row.get("CustomerName") and len(row.get("CustomerName", "").split()) > 1 else "",
                    "rics_customer_id": row.get("CustomerId", "").strip(),
                    "rics_account_number": row.get("AccountNumber", "").strip(),
                }
                
                # Add phone if available
                if phone:
                    profile_attrs["phone_number"] = phone
                
                # Remove empty values
                profile_attrs = {k: v for k, v in profile_attrs.items() if v}
                
                # Build purchase event properties
                # Include order_id and value for Optimizely to recognize as purchase event
                amount_paid_str = row.get("AmountPaid", "").strip()
                try:
                    amount_paid_float = float(amount_paid_str) if amount_paid_str else 0.0
                except (ValueError, TypeError):
                    amount_paid_float = 0.0
                
                event_props = {
                    "ticket_number": ticket_number,
                    "store_code": row.get("StoreCode", "").strip(),
                    "terminal_id": row.get("TerminalId", "").strip(),
                    "cashier": row.get("Cashier", "").strip(),
                    "sku": row.get("Sku", "").strip(),
                    "description": row.get("Description", "").strip(),
                    "quantity": row.get("Quantity", "").strip(),
                    "amount_paid": amount_paid_str,  # Keep original string value
                    "discount": row.get("Discount", "").strip(),
                    "department": row.get("Department", "").strip(),
                    "supplier_name": row.get("SupplierName", "").strip(),
                }
                
                # Add order_id and value only if ticket_number is not empty (required for Optimizely purchase recognition)
                if ticket_number:
                    event_props["order_id"] = ticket_number
                    event_props["value"] = amount_paid_float
                    event_props["currency"] = "USD"
                
                # Remove empty values
                event_props = {k: v for k, v in event_props.items() if v}
                
                valid_rows += 1
                rows_processed += 1
                
                # ⚡ DEDUPLICATION: Check if we already synced this purchase event to this profile
                # Event key = email + ticket_number + sku + timestamp (unique per purchase event per customer)
                event_key = _generate_event_key(email, ticket_number, event_props.get("sku", ""), purchase_ts)
                is_duplicate = event_key in processed_event_keys
                
                if is_duplicate:
                    skipped_duplicate_events += 1
                    skip_reasons["duplicate_event"] += 1
                    # Only log duplicates in test mode or very small datasets
                    if (RICS_TEST_MODE and rows_processed < 3) or (not RICS_TEST_MODE and total_rows <= 5):
                        print(f"⏭️  Skipping duplicate: {email} (ticket {ticket_number})")
                    continue  # Skip entire row - this purchase event already synced to this profile
                
                # Skip actual posting if DRY_RUN
                if DRY_RUN:
                    # Only log in test mode or very small datasets
                    if (RICS_TEST_MODE and rows_processed < 3) or (not RICS_TEST_MODE and total_rows <= 5):
                        print(f"   [DRY_RUN] Would process: {email} - ticket {ticket_number}, ${amount_paid_str}")
                    continue
                
                # Step 1: Upsert profile (creates new profile if doesn't exist, updates if exists)
                # This will:
                # - Create profile if it doesn't exist
                # - Update profile if it exists
                # - Subscribe to base_store_purchases_only list if not already subscribed
                # - Respect existing unsubscribes (won't re-subscribe if previously unsubscribed)
                try:
                    action, status_msg, was_subscribed = upsert_profile_with_subscription(
                        email,
                        profile_attrs,
                        OPTIMIZELY_LIST_ID_RICS
                    )
                    
                    posted_profiles += 1
                    if was_subscribed:
                        subscribed_to_lists += 1
                    
                    # Only log first few rows in test mode or if very small dataset
                    if (RICS_TEST_MODE and rows_processed < 3) or (not RICS_TEST_MODE and total_rows <= 5):
                        print(f"   Row {row_idx}: {action} profile for {email} - {status_msg}")
                except Exception as e:
                    print(f"❌ Error upserting profile for {email} (row {row_idx}): {e}")
                    # Continue to try posting event even if profile update fails
                
                # Step 2: Build purchase event payload for batch
                # This purchase event will be posted to the profile (whether new or existing)
                event_payload = {
                    "type": OPTIMIZELY_EVENT_NAME,
                    "timestamp": purchase_ts or datetime.now(timezone.utc).isoformat(),
                    "identifiers": {
                        "email": email
                    },
                    "properties": event_props
                }
                event_batch.append(event_payload)
                event_batch_keys.append(event_key)  # Track keys for deduplication (only mark as processed after successful post)
                
                # Post batch when it reaches the batch size
                if len(event_batch) >= EVENT_BATCH_SIZE:
                    try:
                        status_code, response_text = post_events_batch(event_batch)
                        if status_code in (200, 202):
                            # ✅ SUCCESS: Purchase events posted to Optimizely
                            # Only mark as processed AFTER successful API response (200/202)
                            # This ensures we don't lose customers if API call fails
                            for key in event_batch_keys:
                                new_event_keys.add(key)
                            posted_events += len(event_batch)
                            
                            # Only log batch posts in test mode or very small datasets
                            if (RICS_TEST_MODE and rows_processed < 10) or (not RICS_TEST_MODE and total_rows <= 10):
                                print(f"   ✅ Posted batch of {len(event_batch)} events (status: {status_code})")
                        else:
                            print(f"⚠️ Event batch post failed: {status_code} - {response_text[:200]}")
                            print(f"   ⚠️ NOT marking {len(event_batch)} events as processed (will retry next run)")
                            # Don't add to new_event_keys - will retry next time
                        event_batch = []  # Clear batch
                        event_batch_keys = []  # Clear keys
                    except Exception as e:
                        print(f"❌ Error posting event batch: {e}")
                        print(f"   ⚠️ NOT marking events as processed (will retry next run)")
                        event_batch = []  # Clear batch on error
                        event_batch_keys = []  # Clear keys
                
            except Exception as e:
                print(f"❌ Error processing row {row_idx}: {e}")
                skipped_rows += 1
                skip_reasons["exception"] += 1
                continue
    
    # Flush any remaining events in batch
    if event_batch and not DRY_RUN:
        try:
            status_code, response_text = post_events_batch(event_batch)
            if status_code in (200, 202):
                # ✅ SUCCESS: Only mark as processed AFTER successful post
                for key in event_batch_keys:
                    new_event_keys.add(key)
                posted_events += len(event_batch)
                print(f"\n📦 Posted final batch of {len(event_batch)} purchase events (status: {status_code})")
                print(f"   ✅ Marked {len(event_batch_keys)} events as processed in deduplication log")
            else:
                print(f"\n⚠️ Final event batch post failed: {status_code} - {response_text[:200]}")
                print(f"   ⚠️ NOT marking {len(event_batch)} events as processed (will retry next run)")
            event_batch = []
            event_batch_keys = []
        except Exception as e:
            print(f"\n❌ Error posting final event batch: {e}")
            print(f"   ⚠️ NOT marking events as processed (will retry next run)")
            event_batch = []
            event_batch_keys = []
    
    # Save processed events for next run
    if new_event_keys:
        updated_keys = processed_event_keys.union(new_event_keys)
        save_processed_events(updated_keys)
        print(f"\n💾 Saved {len(new_event_keys)} new event keys to deduplication log")
    else:
        # Preserve existing events even if no new ones
        save_processed_events(processed_event_keys)
        print(f"\n💾 Preserved {len(processed_event_keys)} existing tracked events")
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if RICS_TEST_MODE:
        test_info = f"TEST MODE: {rows_processed} rows processed"
        if RICS_TEST_EMAIL_FILTER:
            test_info += f" (filtered by: {RICS_TEST_EMAIL_FILTER})"
        elif RICS_TEST_NAME:
            test_info += f" (filtered by: {RICS_TEST_NAME})"
        print(f"⚠️  {test_info}")
    print(f"Total rows in CSV: {total_rows}")
    print(f"Valid rows: {valid_rows}")
    print(f"Skipped rows: {skipped_rows}")
    if skipped_rows > 0 and (RICS_TEST_MODE or skipped_rows < 100):
        # Only show skip breakdown for test mode or small datasets
        print(f"Skip reasons:")
        for reason, count in skip_reasons.items():
            if count > 0:
                print(f"  - {reason}: {count}")
    if not DRY_RUN:
        print(f"\n✅ Posted to Optimizely:")
        print(f"   Profiles: {posted_profiles}")
        print(f"   Events: {posted_events}")
        if subscribed_to_lists > 0:
            print(f"   Subscriptions: {subscribed_to_lists}")
        if skipped_duplicate_events > 0:
            print(f"   Duplicates skipped: {skipped_duplicate_events}")
    else:
        print(f"\n[DRY_RUN] No data posted to Optimizely")
    print("=" * 60)
    
    return valid_rows


def run_sync(csv_path: Optional[str] = None):
    """
    Main entry point for RICS sync.
    
    Args:
        csv_path: Optional path to CSV file. If not provided, looks for deduped CSV in current directory.
    """
    if csv_path is None:
        # Default to the deduped CSV created by sync_rics_live.py
        csv_path = "rics_customer_purchase_history_deduped.csv"
        if not os.path.exists(csv_path):
            # Try in optimizely_connector/output
            import glob
            output_files = glob.glob("optimizely_connector/output/rics_customer_purchase_history_*_deduped.csv")
            if output_files:
                # Use most recent
                csv_path = max(output_files, key=os.path.getctime)
                print(f"📂 Found CSV in output directory: {csv_path}")
            else:
                raise FileNotFoundError(
                    f"Could not find RICS CSV file. "
                    f"Expected 'rics_customer_purchase_history_deduped.csv' in current directory, "
                    f"or in optimizely_connector/output/"
                )
    
    return process_rics_purchases(csv_path)


if __name__ == "__main__":
    import sys
    csv_file = sys.argv[1] if len(sys.argv) > 1 else None
    try:
        run_sync(csv_file)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
