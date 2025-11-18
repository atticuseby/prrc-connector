"""
Process RunSignup CSV files from Google Drive and sync to Optimizely.

Reads CSVs from multiple Google Drive folders, maps headers, validates rows,
and posts profile updates and events to Optimizely with correct list routing.
"""

import os
import sys
import json
import csv
import io
import re
import hashlib
from datetime import datetime, timezone
from typing import Dict, Optional, List, Tuple, Set

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

from runsignup_connector.optimizely_client import (
    upsert_profile_with_subscription,
    post_event
)


# Configuration
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
RSU_MAX_FILES = int(os.getenv("RSU_MAX_FILES", "0") or "0")
GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS", "").strip()
RSU_FOLDER_IDS = os.getenv("RSU_FOLDER_IDS", "").strip()
OPTIMIZELY_EVENT_NAME = "runsignup_registration"  # Consistent event type

# TEST MODE configuration
RSU_TEST_MODE = os.getenv("RSU_TEST_MODE", "false").lower() == "true"
RSU_TEST_EMAIL = os.getenv("RSU_TEST_EMAIL", "").strip()
RSU_TEST_MAX_ROWS = 5  # Process only 5 rows in test mode

# Event deduplication
PROCESSED_EVENTS_LOG = os.path.join(os.path.dirname(__file__), "..", "logs", "processed_runsignup_events.json")

# Partner to Optimizely list ID mapping is built dynamically in load_partner_mappings()

# Header mapping: RunSignup CSV headers -> canonical keys
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


def _validate_required_env():
    """Validate that all required environment variables are set."""
    required = ["OPTIMIZELY_API_TOKEN", "GDRIVE_CREDENTIALS"]
    missing = [r for r in required if not os.getenv(r)]
    if missing:
        raise RuntimeError(f"Missing required env: {', '.join(missing)}")


def load_partner_mappings():
    """
    Build partner-to-folder and partner-to-list mappings.
    
    RSU_FOLDER_IDS contains partner IDs (e.g., "1384,1385,1411").
    For each partner, we look up:
    - GDRIVE_FOLDER_ID_{partner_id} → folder_id
    - OPTIMIZELY_LIST_ID_{partner_id} → list_id
    
    Returns:
        Tuple of (enabled_partner_ids, partner_to_folder, partner_to_list)
    """
    rsu_raw = os.getenv("RSU_FOLDER_IDS", "").strip()
    
    print(f"🔍 DEBUG: RSU_FOLDER_IDS raw: {rsu_raw}")
    
    # Parse RSU_FOLDER_IDS as comma-separated partner IDs
    # Strip whitespace and remove 'id_' prefix if present (handles both "1384" and "id_1384" formats)
    raw_ids = [pid.strip() for pid in rsu_raw.split(",") if pid.strip()]
    enabled_partner_ids = []
    for raw_id in raw_ids:
        # Remove 'id_' prefix if present
        partner_id = raw_id.replace("id_", "").replace("ID_", "").strip()
        if partner_id:
            enabled_partner_ids.append(partner_id)
    
    if not enabled_partner_ids:
        raise RuntimeError("RSU_FOLDER_IDS is empty or not set. Set it to comma-separated partner IDs (e.g., '1384,1385,1411')")
    
    print(f"🔍 DEBUG: Parsed partner IDs from RSU_FOLDER_IDS: {', '.join(enabled_partner_ids)}")
    
    # Build partner → folder mapping
    partner_to_folder = {}
    partner_to_list = {}
    
    # Known partners
    known_partners = ["1384", "1385", "1411"]
    
    for partner_id in known_partners:
        folder_id = os.getenv(f"GDRIVE_FOLDER_ID_{partner_id}", "").strip()
        list_id = os.getenv(f"OPTIMIZELY_LIST_ID_{partner_id}", "").strip()
        
        if folder_id:
            partner_to_folder[partner_id] = folder_id
            print(f"🔍 DEBUG: GDRIVE_FOLDER_ID_{partner_id}: {folder_id[-6:]}")
        else:
            print(f"🔍 DEBUG: GDRIVE_FOLDER_ID_{partner_id}: NOT SET")
        
        if list_id:
            partner_to_list[partner_id] = list_id
        else:
            print(f"🔍 DEBUG: OPTIMIZELY_LIST_ID_{partner_id}: NOT SET")
    
    # Validate that every enabled partner has required config
    missing_config = []
    for partner_id in enabled_partner_ids:
        if partner_id not in known_partners:
            missing_config.append(f"Partner {partner_id} is not a known partner (known: {', '.join(known_partners)})")
        elif partner_id not in partner_to_folder:
            missing_config.append(f"Partner {partner_id} missing GDRIVE_FOLDER_ID_{partner_id}")
        elif partner_id not in partner_to_list:
            missing_config.append(f"Partner {partner_id} missing OPTIMIZELY_LIST_ID_{partner_id}")
    
    if missing_config:
        error_msg = "Configuration errors:\n  " + "\n  ".join(missing_config)
        raise RuntimeError(error_msg)
    
    # Log final mapping table
    print("\n📋 Partner → Folder → List mapping:")
    for partner_id in enabled_partner_ids:
        folder_id = partner_to_folder[partner_id]
        list_id = partner_to_list[partner_id]
        print(f"   Partner {partner_id}: folder {folder_id[-6:]} → list {list_id}")
    
    return enabled_partner_ids, partner_to_folder, partner_to_list


def _get_drive_service():
    """Initialize and return Google Drive service."""
    if not GDRIVE_CREDENTIALS:
        raise ValueError("GDRIVE_CREDENTIALS environment variable is not set")
    
    try:
        creds_info = json.loads(GDRIVE_CREDENTIALS)
    except json.JSONDecodeError as e:
        raise ValueError(f"GDRIVE_CREDENTIALS is not valid JSON: {e}")
    
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def drive_list_csvs(drive_service, folder_id: str) -> List[Dict]:
    """
    List CSV files in the Google Drive folder, supporting Shared Drives.
    
    Filters by file name ending in .csv (case-insensitive).
    """
    query = f"'{folder_id}' in parents and trashed = false"
    
    try:
        response = drive_service.files().list(
            q=query,
            fields="files(id,name,modifiedTime,webViewLink)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True
        ).execute()
        
        files = response.get("files", [])
        # Filter by name ending in .csv (case-insensitive)
        return [f for f in files if f.get("name", "").lower().endswith(".csv")]
    except Exception as e:
        raise RuntimeError(f"Failed to list files in Google Drive folder {folder_id[-6:]}: {e}")


def _download_csv(drive_service, file_id: str) -> str:
    """Download a CSV file from Google Drive and return its content as a string."""
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        
        done = False
        while not done:
            _, done = downloader.next_chunk()
        
        file_content.seek(0)
        return file_content.read().decode("utf-8")
    except Exception as e:
        raise RuntimeError(f"Failed to download CSV file {file_id}: {e}")


def _normalize_email(email: str) -> Optional[str]:
    """Validate and normalize email address."""
    if not email:
        return None
    
    email = email.strip().lower()
    
    # Basic email validation
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        return None
    
    return email


def _generate_event_key(email: str, event_props: Dict, registration_ts: Optional[str]) -> str:
    """
    Generate a unique key for an event to use for deduplication.
    
    Uses email + event name + event year + bib + registration timestamp
    to uniquely identify a registration event.
    
    Args:
        email: Normalized email address
        event_props: Event properties dict
        registration_ts: Registration timestamp (ISO format)
        
    Returns:
        SHA256 hash of the event key components
    """
    # Build key from unique identifiers
    key_parts = [
        email.lower().strip(),
        str(event_props.get("event", "")).strip(),
        str(event_props.get("event_year", "")).strip(),
        str(event_props.get("bib", "")).strip(),
        str(registration_ts or "").strip()
    ]
    
    # Join and hash
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
            # Handle both old format (list) and new format (dict with keys)
            if isinstance(data, list):
                return set(data)
            elif isinstance(data, dict) and "events" in data:
                return set(data["events"])
            else:
                return set()
    except (json.JSONDecodeError, IOError) as e:
        print(f"⚠️ Warning: Could not load processed events log: {e}")
        return set()


def save_processed_events(event_keys: Set[str]):
    """
    Save processed event keys to log file.
    
    Args:
        event_keys: Set of event key hashes to save
    """
    os.makedirs(os.path.dirname(PROCESSED_EVENTS_LOG), exist_ok=True)
    
    # Save as JSON with metadata
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
    """Parse registration timestamp and convert to ISO 8601 format."""
    if not ts_str or not ts_str.strip():
        return None
    
    ts_str = ts_str.strip()
    
    # Try common formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S %p",
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(ts_str, fmt)
            # Make timezone-aware (assume UTC if no timezone info)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue
    
    # If all formats fail, use current time
    print(f"⚠️ Could not parse timestamp '{ts_str}', using current time")
    return datetime.now(timezone.utc).isoformat()


def _map_row(row: Dict) -> Tuple[Optional[Dict], Optional[Dict], Optional[str]]:
    """
    Map a CSV row to profile attributes and event properties.
    
    Returns:
        Tuple of (profile_attrs, event_props, registration_ts) or (None, None, None) if row is invalid
    """
    # Normalize headers (case-insensitive)
    normalized_row = {}
    for csv_key, value in row.items():
        csv_key_clean = csv_key.strip()
        if csv_key_clean in HEADER_MAP:
            normalized_row[HEADER_MAP[csv_key_clean]] = value
        else:
            # Keep original key if not in map
            normalized_row[csv_key_clean.lower().replace(" ", "_")] = value
    
    # Extract and validate email
    email = _normalize_email(normalized_row.get("email", ""))
    if not email:
        return None, None, None
    
    # Build profile attributes
    profile_attrs = {
        "first_name": normalized_row.get("first_name", "").strip() or None,
        "last_name": normalized_row.get("last_name", "").strip() or None,
        "rsu_event": normalized_row.get("event_name", "").strip() or None,
        "rsu_event_year": normalized_row.get("event_year", "").strip() or None,
    }
    
    # Remove None values
    profile_attrs = {k: v for k, v in profile_attrs.items() if v is not None}
    
    # Build event properties
    event_props = {
        "event": normalized_row.get("event_name", "").strip() or None,
        "event_year": normalized_row.get("event_year", "").strip() or None,
        "bib": normalized_row.get("bib", "").strip() or None,
        "gender": normalized_row.get("gender", "").strip() or None,
        "age": normalized_row.get("age", "").strip() or None,
        "race": normalized_row.get("race", "").strip() or None,
    }
    
    # Convert age to int if possible
    if event_props.get("age"):
        try:
            event_props["age"] = int(event_props["age"])
        except (ValueError, TypeError):
            event_props["age"] = None
    
    # Convert event_year to int if possible
    if event_props.get("event_year"):
        try:
            event_props["event_year"] = int(event_props["event_year"])
        except (ValueError, TypeError):
            event_props["event_year"] = None
    
    # Remove None/empty values
    event_props = {k: v for k, v in event_props.items() if v not in (None, "", "NULL")}
    
    # Parse timestamp
    registration_ts = _parse_timestamp(normalized_row.get("registration_ts", ""))
    
    return profile_attrs, event_props, registration_ts


def process_runsignup_csvs():
    """Main processing function: read CSVs from Google Drive and sync to Optimizely."""
    
    print("=== RUNSIGNUP CSV PROCESSOR ===")
    print(f"DRY_RUN: {DRY_RUN}")
    print(f"TEST_MODE: {RSU_TEST_MODE}")
    if RSU_TEST_MODE:
        if not RSU_TEST_EMAIL:
            raise RuntimeError("RSU_TEST_MODE is true but RSU_TEST_EMAIL is not set")
        print(f"TEST_EMAIL: {RSU_TEST_EMAIL}")
        print(f"TEST_MAX_ROWS: {RSU_TEST_MAX_ROWS}")
        print("⚠️  TEST MODE: Only processing first 5 rows and overriding emails with TEST_EMAIL")
    print(f"Max files to process: {RSU_MAX_FILES if RSU_MAX_FILES > 0 else 'all'}")
    print()
    
    # Validate required env vars
    _validate_required_env()
    
    # Load partner mappings (RSU_FOLDER_IDS contains partner IDs)
    enabled_partner_ids, partner_to_folder, partner_to_list = load_partner_mappings()
    
    # Self-check: assert all enabled partners have complete config
    for partner_id in enabled_partner_ids:
        if partner_id not in partner_to_folder:
            raise RuntimeError(f"Missing GDRIVE_FOLDER_ID_{partner_id} for partner {partner_id}")
        if partner_id not in partner_to_list:
            raise RuntimeError(f"Missing OPTIMIZELY_LIST_ID_{partner_id} for partner {partner_id}")
    
    print()
    
    # Initialize Google Drive service
    try:
        drive_service = _get_drive_service()
        print("✅ Connected to Google Drive")
    except Exception as e:
        print(f"❌ Failed to connect to Google Drive: {e}")
        raise
    
    # Collect files from all partner folders
    files_global = []
    folders_processed = 0
    
    for partner_id in enabled_partner_ids:
        folder_id = partner_to_folder[partner_id]
        list_id = partner_to_list[partner_id]
        
        print(f"\n📁 Processing partner {partner_id}:")
        print(f"   Folder: {folder_id[-6:]}")
        print(f"   List: {list_id}")
        
        # List CSV files in this folder
        try:
            files = drive_list_csvs(drive_service, folder_id)
            print(f"   Found {len(files)} CSV file(s)")
            
            # Log file names
            if files:
                file_names = [f.get("name", "unknown") for f in files]
                print(f"   Files: {', '.join(file_names[:5])}{'...' if len(file_names) > 5 else ''}")
            
            # Attach metadata for routing
            for f in files:
                f["_partner_id"] = partner_id
                f["_folder_id"] = folder_id
                f["_list_id"] = list_id
            
            files_global.extend(files)
            folders_processed += 1
        except Exception as e:
            print(f"❌ Error listing files in folder {folder_id[-6:]}: {e}")
            continue
    
    if not files_global:
        print(f"\n⚠️ No CSV files found in any folder (processed {folders_processed} folder(s))")
        return 0
    
    # Sort by modifiedTime (newest first) and limit
    files_global.sort(key=lambda f: f.get("modifiedTime", ""), reverse=True)
    
    if RSU_MAX_FILES > 0:
        files_global = files_global[:RSU_MAX_FILES]
    
    print(f"\n📂 Total CSVs selected: {len(files_global)}")
    
    # Log selected files with full details
    print("\n📋 Files to process:")
    for idx, f in enumerate(files_global, 1):
        modified_time = f.get("modifiedTime", "unknown")
        print(f"   {idx}. {f['name']}")
        print(f"      Partner: {f['_partner_id']} | Folder: {f['_folder_id'][-6:]} | List: {f['_list_id']}")
        print(f"      Modified: {modified_time}")
        if f.get("webViewLink"):
            print(f"      Link: {f['webViewLink']}")
    
    print()
    
    # Load processed events for deduplication
    processed_event_keys = load_processed_events()
    new_event_keys = set()  # Track new events processed in this run
    skipped_duplicate_events = 0
    
    if not DRY_RUN:
        print(f"📋 Loaded {len(processed_event_keys)} previously processed events for deduplication")
    
    # Process each file
    total_rows = 0
    valid_rows = 0
    skipped_rows = 0
    posted_profiles = 0
    posted_events = 0
    subscribed_to_lists = 0  # Track successful list subscriptions
    rows_processed = 0  # Track rows that were actually processed (valid rows)
    sample_rows_by_partner = {}  # Store first 2 mapped rows per partner for DRY_RUN logging
    processed_files = []  # Track which files were actually processed
    detailed_log_count = 0  # Track how many rows we've logged in detail (first few rows)
    MAX_DETAILED_LOGS = 5  # Log first 5 rows in detail
    
    for file_info in files_global:
        file_id = file_info["id"]
        file_name = file_info["name"]
        partner_id = file_info["_partner_id"]
        folder_id = file_info["_folder_id"]
        list_id = file_info["_list_id"]  # Already attached during collection
        
        print(f"\n{'='*60}")
        print(f"📄 Processing file: {file_name}")
        print(f"   Partner: {partner_id} | Folder: {folder_id[-6:]} | List: {list_id}")
        print(f"{'='*60}")
        
        try:
            csv_content = _download_csv(drive_service, file_id)
            print(f"✅ Downloaded {file_name} ({len(csv_content)} bytes)")
        except Exception as e:
            print(f"❌ Failed to download {file_name}: {e}")
            continue
        
        # Parse CSV
        reader = csv.DictReader(io.StringIO(csv_content))
        file_row_count = 0
        file_valid_rows = 0
        file_skipped_rows = 0
        
        for row_idx, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            # TEST MODE: Only process first 5 rows
            if RSU_TEST_MODE and rows_processed >= RSU_TEST_MAX_ROWS:
                print(f"\n⚠️  TEST MODE: Reached max rows ({RSU_TEST_MAX_ROWS}), stopping processing")
                break
            
            file_row_count += 1
            total_rows += 1
            
            try:
                profile_attrs, event_props, registration_ts = _map_row(row)
                
                if profile_attrs is None:
                    skipped_rows += 1
                    file_skipped_rows += 1
                    continue
                
                valid_rows += 1
                file_valid_rows += 1
                rows_processed += 1
                
                # Get original email before TEST MODE override
                original_email = _normalize_email(row.get("Email Address", ""))
                
                # TEST MODE: Override email with test email
                if RSU_TEST_MODE:
                    email = RSU_TEST_EMAIL
                    if detailed_log_count < MAX_DETAILED_LOGS:
                        print(f"\n🧪 TEST MODE: Overriding email {original_email} → {email}")
                else:
                    email = original_email
                
                # Store sample rows for DRY_RUN (first 2 per partner)
                if DRY_RUN:
                    if partner_id not in sample_rows_by_partner:
                        sample_rows_by_partner[partner_id] = []
                    if len(sample_rows_by_partner[partner_id]) < 2:
                        sample_rows_by_partner[partner_id].append({
                            "email": email,
                            "original_email": original_email if RSU_TEST_MODE else email,
                            "profile_attrs": profile_attrs,
                            "event_props": event_props,
                            "timestamp": registration_ts,
                            "list_id": list_id,
                            "partner_id": partner_id,
                            "file_name": file_name
                        })
                    # Log subscription in DRY_RUN mode
                    if list_id:
                        print(f"[DRY_RUN] Would upsert profile {email} and subscribe to list {list_id}")
                
                # Skip actual posting if DRY_RUN
                if DRY_RUN:
                    continue
                
                # Upsert profile with idempotent subscription logic
                should_log_detail = detailed_log_count < MAX_DETAILED_LOGS
                if should_log_detail:
                    print(f"\n📝 Processing row {row_idx} (email: {email}):")
                
                try:
                    action, status_msg, was_subscribed = upsert_profile_with_subscription(
                        email,
                        profile_attrs,
                        list_id
                    )
                    
                    posted_profiles += 1
                    if was_subscribed:
                        subscribed_to_lists += 1
                    
                    if should_log_detail:
                        print(f"   Profile: {action} - {status_msg}")
                    elif detailed_log_count == 0:
                        # Log first row in summary format
                        print(f"   First row: {email} - {action} - {status_msg}")
                    
                    detailed_log_count += 1
                    
                except Exception as e:
                    print(f"❌ Error upserting profile for {email} (row {row_idx} in {file_name}): {e}")
                    if should_log_detail:
                        print(f"   Error details: {str(e)}")
                
                # Generate event key for deduplication
                event_key = _generate_event_key(email, event_props, registration_ts)
                
                # Check if event was already processed
                is_duplicate = event_key in processed_event_keys
                
                if is_duplicate:
                    skipped_duplicate_events += 1
                    if detailed_log_count < MAX_DETAILED_LOGS:
                        print(f"   Event: Skipped (already processed)")
                else:
                    # Post event (use consistent event type: runsignup_registration)
                    try:
                        status_code, response_text = post_event(
                            email,
                            OPTIMIZELY_EVENT_NAME,  # "runsignup_registration"
                            event_props,
                            registration_ts
                        )
                        if status_code in (200, 202):
                            posted_events += 1
                            new_event_keys.add(event_key)  # Mark as processed
                            if detailed_log_count <= MAX_DETAILED_LOGS:
                                print(f"   Event: Posted {OPTIMIZELY_EVENT_NAME} event")
                        else:
                            print(f"⚠️ Event post failed for {email}: {status_code} - {response_text[:200]}")
                    except Exception as e:
                        print(f"❌ Error posting event for {email} (row {row_idx} in {file_name}): {e}")
                    
            except Exception as e:
                print(f"❌ Error processing row {row_idx} in {file_name}: {e}")
                skipped_rows += 1
                continue
        
        # Log file processing summary
        print(f"\n✅ Completed {file_name}:")
        print(f"   Total rows in file: {file_row_count}")
        print(f"   Valid rows: {file_valid_rows}")
        print(f"   Skipped rows: {file_skipped_rows}")
        if not DRY_RUN:
            print(f"   Posted to Optimizely: {file_valid_rows} profiles, {file_valid_rows} events")
        processed_files.append({
            "name": file_name,
            "partner_id": partner_id,
            "folder_id": folder_id[-6:],
            "list_id": list_id,
            "total_rows": file_row_count,
            "valid_rows": file_valid_rows,
            "skipped_rows": file_skipped_rows
        })
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Folders processed: {folders_processed}")
    print(f"Files processed: {len(processed_files)}")
    print(f"\nFiles processed:")
    for f in processed_files:
        print(f"  • {f['name']}")
        print(f"    Partner: {f['partner_id']} | Folder: {f['folder_id']} | List: {f['list_id']}")
        print(f"    Rows: {f.get('total_rows', 0)} total, {f.get('valid_rows', 0)} valid, {f.get('skipped_rows', 0)} skipped")
    print(f"\nOverall:")
    print(f"  Total rows: {total_rows}")
    print(f"  Valid rows: {valid_rows}")
    print(f"  Skipped rows: {skipped_rows}")
    if not DRY_RUN:
        print(f"  Posted profiles: {posted_profiles}")
        print(f"  Posted events: {posted_events}")
        print(f"  Skipped duplicate events: {skipped_duplicate_events}")
        print(f"  Subscribed to lists: {subscribed_to_lists}")
        
        # Save processed events for next run (preserve existing + add new)
        updated_keys = processed_event_keys.union(new_event_keys)
        if new_event_keys:
            save_processed_events(updated_keys)
            print(f"\n💾 Saved {len(new_event_keys)} new event keys to deduplication log")
            print(f"   Total tracked events: {len(updated_keys)}")
        elif len(processed_event_keys) > 0:
            # Even if no new events, save to preserve existing tracked events
            save_processed_events(updated_keys)
            print(f"\n💾 Preserved {len(processed_event_keys)} existing tracked events")
    
    if RSU_TEST_MODE:
        print(f"\n⚠️  TEST MODE was enabled - only processed {rows_processed} rows with email override to {RSU_TEST_EMAIL}")
    print(f"  Rows processed: {rows_processed}")
    print(f"  DRY_RUN: {DRY_RUN}")
    print("=" * 60)
    
    # Print sample rows if DRY_RUN (first 2 per partner)
    if DRY_RUN and sample_rows_by_partner:
        print("\n📋 Sample mapped rows (first 2 per partner):")
        for partner_id, samples in sample_rows_by_partner.items():
            # Get list_id from first sample (all samples from same partner have same list_id)
            list_id = samples[0].get('list_id', 'N/A') if samples else 'N/A'
            print(f"\n  Partner {partner_id} (List: {list_id}):")
            for i, sample in enumerate(samples, 1):
                print(f"\n    Row {i} from {sample.get('file_name', 'unknown')}:")
                print(f"      Email: {sample['email']}")
                print(f"      Profile attrs: {json.dumps(sample['profile_attrs'], indent=8)}")
                print(f"      Event props: {json.dumps(sample['event_props'], indent=8)}")
                print(f"      Timestamp: {sample['timestamp']}")
    
    # Return rows_processed for main script to use
    return rows_processed


if __name__ == "__main__":
    try:
        process_runsignup_csvs()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)
