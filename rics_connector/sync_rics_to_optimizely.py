"""
Process RICS purchase history CSV and sync to Optimizely.

Reads purchase history CSV, posts purchase events, and updates customer profiles
with proper subscription handling (respects unsubscribe states).
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
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
OPTIMIZELY_LIST_ID_RICS = os.getenv("OPTIMIZELY_LIST_ID_RICS", "").strip()
OPTIMIZELY_EVENT_NAME = "rics_purchase"  # Event type for purchases

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
    print(f"CSV file: {csv_path}")
    print()
    
    # Validate required env vars
    if not os.getenv("OPTIMIZELY_API_TOKEN"):
        raise RuntimeError("Missing required env: OPTIMIZELY_API_TOKEN")
    
    if not OPTIMIZELY_LIST_ID_RICS:
        print("⚠️ OPTIMIZELY_LIST_ID_RICS not set - customers will not be subscribed to list")
    else:
        print(f"📋 Subscribing customers to list: {OPTIMIZELY_LIST_ID_RICS}")
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
    posted_profiles = 0
    posted_events = 0
    subscribed_to_lists = 0
    
    # Batch event collection
    event_batch = []
    
    # Process CSV
    print(f"📄 Processing CSV: {csv_path}")
    with open(csv_path, "r", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row_idx, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
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
                email = _normalize_email(row.get("CustomerEmail", ""))
                phone = row.get("CustomerPhone", "").strip()
                
                if not email:
                    skipped_rows += 1
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
                event_props = {
                    "ticket_number": ticket_number,
                    "store_code": row.get("StoreCode", "").strip(),
                    "terminal_id": row.get("TerminalId", "").strip(),
                    "cashier": row.get("Cashier", "").strip(),
                    "sku": row.get("Sku", "").strip(),
                    "description": row.get("Description", "").strip(),
                    "quantity": row.get("Quantity", "").strip(),
                    "amount_paid": row.get("AmountPaid", "").strip(),
                    "discount": row.get("Discount", "").strip(),
                    "department": row.get("Department", "").strip(),
                    "supplier_name": row.get("SupplierName", "").strip(),
                }
                
                # Remove empty values
                event_props = {k: v for k, v in event_props.items() if v}
                
                valid_rows += 1
                
                # ⚡ OPTIMIZATION: Check event deduplication EARLY (before any API calls)
                event_key = _generate_event_key(email, ticket_number, event_props.get("sku", ""), purchase_ts)
                is_duplicate = event_key in processed_event_keys
                
                if is_duplicate:
                    skipped_duplicate_events += 1
                    if total_rows <= 5:
                        print(f"⏭️  Skipping duplicate purchase event for {email} (ticket {ticket_number})")
                    continue  # Skip entire row - no API calls needed
                
                # Skip actual posting if DRY_RUN
                if DRY_RUN:
                    if total_rows <= 5:
                        print(f"\n[DRY_RUN] Would process purchase:")
                        print(f"   Email: {email}")
                        print(f"   Ticket: {ticket_number}")
                        print(f"   SKU: {event_props.get('sku', 'N/A')}")
                        print(f"   Amount: {event_props.get('amount_paid', 'N/A')}")
                    continue
                
                # Upsert profile with idempotent subscription logic
                if OPTIMIZELY_LIST_ID_RICS:
                    try:
                        action, status_msg, was_subscribed = upsert_profile_with_subscription(
                            email,
                            profile_attrs,
                            OPTIMIZELY_LIST_ID_RICS
                        )
                        
                        posted_profiles += 1
                        if was_subscribed:
                            subscribed_to_lists += 1
                        
                        if total_rows <= 5:
                            print(f"\n📝 Processing row {row_idx} (email: {email}):")
                            print(f"   Profile: {action} - {status_msg}")
                    except Exception as e:
                        print(f"❌ Error upserting profile for {email} (row {row_idx}): {e}")
                
                # Build purchase event payload for batch
                event_payload = {
                    "type": OPTIMIZELY_EVENT_NAME,
                    "timestamp": purchase_ts or datetime.now(timezone.utc).isoformat(),
                    "identifiers": {
                        "email": email
                    },
                    "properties": event_props
                }
                event_batch.append(event_payload)
                new_event_keys.add(event_key)  # Mark as processed
                
                # Post batch when it reaches the batch size
                if len(event_batch) >= EVENT_BATCH_SIZE:
                    try:
                        status_code, response_text = post_events_batch(event_batch)
                        if status_code in (200, 202):
                            posted_events += len(event_batch)
                            if total_rows <= 5:
                                print(f"   Events: Posted batch of {len(event_batch)} purchase events")
                        else:
                            print(f"⚠️ Event batch post failed: {status_code} - {response_text[:200]}")
                        event_batch = []  # Clear batch
                    except Exception as e:
                        print(f"❌ Error posting event batch: {e}")
                        event_batch = []  # Clear batch on error
                
            except Exception as e:
                print(f"❌ Error processing row {row_idx}: {e}")
                skipped_rows += 1
                continue
    
    # Flush any remaining events in batch
    if event_batch and not DRY_RUN:
        try:
            status_code, response_text = post_events_batch(event_batch)
            if status_code in (200, 202):
                posted_events += len(event_batch)
                print(f"\n📦 Posted final batch of {len(event_batch)} purchase events")
            else:
                print(f"\n⚠️ Final event batch post failed: {status_code} - {response_text[:200]}")
            event_batch = []
        except Exception as e:
            print(f"\n❌ Error posting final event batch: {e}")
            event_batch = []
    
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
    print(f"Total rows: {total_rows}")
    print(f"Valid rows: {valid_rows}")
    print(f"Skipped rows: {skipped_rows}")
    if not DRY_RUN:
        print(f"Posted profiles: {posted_profiles}")
        print(f"Posted events: {posted_events}")
        print(f"Skipped duplicate events: {skipped_duplicate_events}")
        print(f"Subscribed to lists: {subscribed_to_lists}")
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
