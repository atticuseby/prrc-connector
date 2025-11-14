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
from datetime import datetime, timezone
from typing import Dict, Optional, List, Tuple

# Add parent directory to path for imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

from runsignup_connector.optimizely_client import post_profile, post_event


# Configuration
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
RSU_MAX_FILES = int(os.getenv("RSU_MAX_FILES", "0") or "0")
GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS", "").strip()
RSU_FOLDER_IDS = os.getenv("RSU_FOLDER_IDS", "").strip()
OPTIMIZELY_EVENT_NAME = os.getenv("OPTIMIZELY_EVENT_NAME", "registration").strip()

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
    
    # Process each file
    total_rows = 0
    valid_rows = 0
    skipped_rows = 0
    posted_profiles = 0
    posted_events = 0
    rows_processed = 0  # Track rows that were actually processed (valid rows)
    sample_rows_by_partner = {}  # Store first 2 mapped rows per partner for DRY_RUN logging
    processed_files = []  # Track which files were actually processed
    
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
        
        for row_idx, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            file_row_count += 1
            total_rows += 1
            
            try:
                profile_attrs, event_props, registration_ts = _map_row(row)
                
                if profile_attrs is None:
                    skipped_rows += 1
                    continue
                
                valid_rows += 1
                rows_processed += 1
                
                # Store sample rows for DRY_RUN (first 2 per partner)
                if DRY_RUN:
                    if partner_id not in sample_rows_by_partner:
                        sample_rows_by_partner[partner_id] = []
                    if len(sample_rows_by_partner[partner_id]) < 2:
                        sample_rows_by_partner[partner_id].append({
                            "email": _normalize_email(row.get("Email Address", "")),
                            "profile_attrs": profile_attrs,
                            "event_props": event_props,
                            "timestamp": registration_ts,
                            "list_id": list_id,
                            "partner_id": partner_id,
                            "file_name": file_name
                        })
                
                # Skip actual posting if DRY_RUN
                if DRY_RUN:
                    continue
                
                # Post profile update
                try:
                    email = _normalize_email(row.get("Email Address", ""))
                    status_code, response_text = post_profile(email, profile_attrs, list_id)
                    if status_code in (200, 202):
                        posted_profiles += 1
                    else:
                        print(f"⚠️ Profile post failed for {email}: {status_code} - {response_text[:200]}")
                except Exception as e:
                    print(f"❌ Error posting profile for row {row_idx} in {file_name}: {e}")
                
                # Post event
                try:
                    email = _normalize_email(row.get("Email Address", ""))
                    status_code, response_text = post_event(
                        email,
                        OPTIMIZELY_EVENT_NAME,
                        event_props,
                        registration_ts,
                        list_id
                    )
                    if status_code in (200, 202):
                        posted_events += 1
                    else:
                        print(f"⚠️ Event post failed for {email}: {status_code} - {response_text[:200]}")
                except Exception as e:
                    print(f"❌ Error posting event for row {row_idx} in {file_name}: {e}")
                    
            except Exception as e:
                print(f"❌ Error processing row {row_idx} in {file_name}: {e}")
                skipped_rows += 1
                continue
        
        # Log file processing summary
        print(f"\n✅ Completed {file_name}:")
        print(f"   Total rows in file: {file_row_count}")
        print(f"   Valid rows processed: {valid_rows - sum(f.get('valid_rows', 0) for f in processed_files)}")
        processed_files.append({
            "name": file_name,
            "partner_id": partner_id,
            "folder_id": folder_id[-6:],
            "list_id": list_id,
            "total_rows": file_row_count,
            "valid_rows": valid_rows - sum(f.get('valid_rows', 0) for f in processed_files)
        })
    
    # Print summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Folders processed: {folders_processed}")
    print(f"Files processed: {len(processed_files)}")
    print(f"\nFiles processed:")
    for f in processed_files:
        print(f"  • {f['name']} (Partner {f['partner_id']}, Folder {f['folder_id']}, List {f['list_id']})")
        print(f"    Rows: {f.get('total_rows', 0)} total, {f.get('valid_rows', 0)} valid")
    print(f"\nOverall:")
    print(f"  Total rows: {total_rows}")
    print(f"  Valid rows: {valid_rows}")
    print(f"  Skipped rows: {skipped_rows}")
    if not DRY_RUN:
        print(f"  Posted profiles: {posted_profiles}")
        print(f"  Posted events: {posted_events}")
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
