"""
Process RunSignup CSV files from Google Drive and sync to Optimizely.

Reads CSVs from a Google Drive folder, maps headers, validates rows,
and posts profile updates and events to Optimizely.
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
RSU_MAX_FILES = int(os.getenv("RSU_MAX_FILES", "1"))
GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS", "").strip()
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "").strip()
OPTIMIZELY_EVENT_NAME = os.getenv("OPTIMIZELY_EVENT_NAME", "registration").strip()

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


def _get_drive_service():
    """Initialize and return Google Drive service."""
    if not GDRIVE_CREDENTIALS:
        raise ValueError("GDRIVE_CREDENTIALS environment variable is not set")
    if not GDRIVE_FOLDER_ID:
        raise ValueError("GDRIVE_FOLDER_ID environment variable is not set")
    
    try:
        creds_info = json.loads(GDRIVE_CREDENTIALS)
    except json.JSONDecodeError as e:
        raise ValueError(f"GDRIVE_CREDENTIALS is not valid JSON: {e}")
    
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def _list_csv_files(drive_service, folder_id: str) -> List[Dict]:
    """List CSV files in the Google Drive folder, sorted by creation time (newest first)."""
    query = f"'{folder_id}' in parents and trashed = false and mimeType = 'text/csv'"
    
    try:
        response = drive_service.files().list(
            q=query,
            fields="files(id,name,createdTime,modifiedTime)",
            orderBy="createdTime desc",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        
        files = response.get("files", [])
        return files[:RSU_MAX_FILES]  # Limit to most recent N files
    except Exception as e:
        raise RuntimeError(f"Failed to list files in Google Drive folder: {e}")


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
    
    # If all formats fail, try to parse with dateutil (if available) or return current time
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
        return None, None
    
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
    print(f"Max files to process: {RSU_MAX_FILES}")
    print()
    
    # Initialize Google Drive service
    try:
        drive_service = _get_drive_service()
        print(f"✅ Connected to Google Drive (folder: {GDRIVE_FOLDER_ID})")
    except Exception as e:
        print(f"❌ Failed to connect to Google Drive: {e}")
        raise
    
    # List CSV files
    try:
        csv_files = _list_csv_files(drive_service, GDRIVE_FOLDER_ID)
        if not csv_files:
            print("⚠️ No CSV files found in Google Drive folder")
            return
        
        print(f"📂 Found {len(csv_files)} CSV file(s) to process")
    except Exception as e:
        print(f"❌ Failed to list CSV files: {e}")
        raise
    
    # Process each file
    total_rows = 0
    valid_rows = 0
    skipped_rows = 0
    posted_profiles = 0
    posted_events = 0
    sample_rows = []  # Store first 2 mapped rows for DRY_RUN logging
    
    for file_info in csv_files:
        file_id = file_info["id"]
        file_name = file_info["name"]
        
        print(f"\n📄 Processing: {file_name}")
        
        try:
            csv_content = _download_csv(drive_service, file_id)
        except Exception as e:
            print(f"❌ Failed to download {file_name}: {e}")
            continue
        
        # Parse CSV
        reader = csv.DictReader(io.StringIO(csv_content))
        
        for row_idx, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
            total_rows += 1
            
            try:
                profile_attrs, event_props, registration_ts = _map_row(row)
                
                if profile_attrs is None:
                    skipped_rows += 1
                    continue
                
                valid_rows += 1
                
                # Store sample rows for DRY_RUN
                if DRY_RUN and len(sample_rows) < 2:
                    sample_rows.append({
                        "email": _normalize_email(row.get("Email Address", "")),
                        "profile_attrs": profile_attrs,
                        "event_props": event_props,
                        "timestamp": registration_ts
                    })
                
                # Skip actual posting if DRY_RUN
                if DRY_RUN:
                    continue
                
                # Post profile update
                try:
                    email = _normalize_email(row.get("Email Address", ""))
                    status_code, response_text = post_profile(email, profile_attrs)
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
                        registration_ts
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
    
    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    print(f"Files processed: {len(csv_files)}")
    print(f"Total rows: {total_rows}")
    print(f"Valid rows: {valid_rows}")
    print(f"Skipped rows: {skipped_rows}")
    print(f"Posted profiles: {posted_profiles}")
    print(f"Posted events: {posted_events}")
    print(f"DRY_RUN: {DRY_RUN}")
    print("=" * 50)
    
    # Print sample rows if DRY_RUN
    if DRY_RUN and sample_rows:
        print("\n📋 Sample mapped rows (first 2):")
        for i, sample in enumerate(sample_rows, 1):
            print(f"\n  Row {i}:")
            print(f"    Email: {sample['email']}")
            print(f"    Profile attrs: {json.dumps(sample['profile_attrs'], indent=6)}")
            print(f"    Event props: {json.dumps(sample['event_props'], indent=6)}")
            print(f"    Timestamp: {sample['timestamp']}")


if __name__ == "__main__":
    try:
        process_runsignup_csvs()
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        sys.exit(1)
