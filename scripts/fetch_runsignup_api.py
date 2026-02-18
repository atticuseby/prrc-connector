"""
Fetch RunSignUp participant data via API and upload CSV to Google Drive.

Replaces the Selenium-based download_all_runsignup_csvs.py with direct API calls.
Produces one CSV per partner (matching the format process_runsignup_csvs.py expects)
and uploads it to the partner's Google Drive folder.

Required env vars:
    RUNSIGNUP_API_KEY
    RUNSIGNUP_API_SECRET
    RUNSIGNUP_PARTNER_IDS      e.g. "1384,1385,1411"
    GDRIVE_CREDENTIALS         service account JSON
    GDRIVE_FOLDER_ID_<id>      one per partner ID, e.g. GDRIVE_FOLDER_ID_1384
"""

import csv
import io
import json
import os
import sys
import time
from datetime import datetime

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

API_KEY = os.getenv("RUNSIGNUP_API_KEY", "")
API_SECRET = os.getenv("RUNSIGNUP_API_SECRET", "")
PARTNER_IDS = [p.strip() for p in os.getenv("RUNSIGNUP_PARTNER_IDS", "").split(",") if p.strip()]
GDRIVE_CREDENTIALS = os.getenv("GDRIVE_CREDENTIALS", "")

BASE_URL = "https://runsignup.com/Rest"
RESULTS_PER_PAGE = 2500

# Must match the headers in process_runsignup_csvs.py HEADER_MAP
CSV_HEADERS = [
    "First Name", "Middle Name", "Last Name", "Email Address",
    "Event", "Event Year", "Registration Date", "Bib", "Gender", "Age", "Race",
]


# ---------------------------------------------------------------------------
# Google Drive helpers
# ---------------------------------------------------------------------------

def _get_drive_service():
    creds_info = json.loads(GDRIVE_CREDENTIALS)
    creds = service_account.Credentials.from_service_account_info(
        creds_info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


def _upload_csv(drive_service, folder_id: str, filename: str, csv_content: str):
    """Upload a CSV string to Drive, overwriting an existing file of the same name."""
    query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
    existing = drive_service.files().list(
        q=query,
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute().get("files", [])

    media = MediaIoBaseUpload(
        io.BytesIO(csv_content.encode("utf-8")),
        mimetype="text/csv",
        resumable=False,
    )

    if existing:
        file_id = existing[0]["id"]
        drive_service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True,
        ).execute()
        print(f"  ✅ Overwrote {filename} in Drive")
    else:
        drive_service.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        ).execute()
        print(f"  ✅ Created {filename} in Drive")


# ---------------------------------------------------------------------------
# RunSignUp API helpers
# ---------------------------------------------------------------------------

def _api_get(path: str, params: dict) -> dict:
    """Authenticated GET against the RunSignUp REST API."""
    full_params = {
        "api_key": API_KEY,
        "api_secret": API_SECRET,
        "format": "json",
        **params,
    }
    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = requests.get(url, params=full_params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _get_partner_races(partner_id: str) -> list:
    """Return all races (with embedded events) for a partner, handling pagination."""
    all_races = []
    page = 1
    while True:
        data = _api_get("/races", {
            "partner_id": partner_id,
            "events": "T",            # include events list in each race
            "results_per_page": 500,
            "page": page,
            "include_waiver": "F",
            "include_questions": "F",
            "include_addr": "F",
        })
        page_races = data.get("races", [])
        all_races.extend(page_races)
        if len(page_races) < 500:
            break
        page += 1
        time.sleep(0.2)
    return all_races


def _get_participants_for_race(race_id: int, event_ids: list) -> list:
    """Fetch all participants for a race, paginating through all pages."""
    all_participants = []
    page = 1
    while True:
        data = _api_get(f"/race/{race_id}/participants", {
            "event_id": ",".join(str(e) for e in event_ids),
            "results_per_page": RESULTS_PER_PAGE,
            "page": page,
            "sort": "registration_id ASC",
            "include_teams": "F",
            "include_checkin": "F",
            "include_fundraiser": "F",
        })
        page_results = data.get("participants", [])
        all_participants.extend(page_results)
        if len(page_results) < RESULTS_PER_PAGE:
            break
        page += 1
        time.sleep(0.25)
    return all_participants


# ---------------------------------------------------------------------------
# Data mapping
# ---------------------------------------------------------------------------

def _extract_year(date_str: str) -> str:
    """Pull the 4-digit year from a date string like '2024-06-07 08:30:00' or '9/7/2021 15:15'."""
    if not date_str:
        return ""
    date_str = date_str.strip()
    # ISO format: YYYY-MM-DD ...
    if len(date_str) >= 4 and date_str[:4].isdigit() and (len(date_str) == 4 or date_str[4] == "-"):
        return date_str[:4]
    # M/D/YYYY or MM/DD/YYYY format
    parts = date_str.split("/")
    if len(parts) == 3:
        year_part = parts[2].split(" ")[0]
        if len(year_part) == 4 and year_part.isdigit():
            return year_part
    return ""


def _participant_to_row(participant: dict, race_name: str, event_name: str, event_year: str) -> dict:
    """Map a RunSignUp API participant record to a flat CSV row."""
    user = participant.get("user", {})
    return {
        "First Name": user.get("first_name", ""),
        "Middle Name": user.get("middle_name", ""),
        "Last Name": user.get("last_name", ""),
        "Email Address": user.get("email", ""),
        "Event": event_name,
        "Event Year": event_year,
        "Registration Date": participant.get("registration_date", ""),
        "Bib": participant.get("bib_num", "") or "",
        "Gender": user.get("gender", ""),
        "Age": participant.get("age", "") or "",
        "Race": race_name,
    }


# ---------------------------------------------------------------------------
# Per-partner fetch
# ---------------------------------------------------------------------------

def _fetch_partner(partner_id: str, folder_id: str, drive_service) -> int:
    """Fetch all participants for a partner and upload one CSV to Drive."""
    print(f"\n{'='*50}")
    print(f"Partner {partner_id}")
    print(f"{'='*50}")

    races = _get_partner_races(partner_id)
    print(f"  Found {len(races)} races")

    all_rows = []

    for race_entry in races:
        # The API wraps each race in a {"race": {...}} envelope
        race = race_entry.get("race", race_entry)
        race_id = race.get("race_id")
        race_name = race.get("name", f"Race {race_id}")
        events = race.get("events", [])

        if not events:
            print(f"  ⚠️  Race {race_id} ({race_name}): no events, skipping")
            continue

        event_ids = [e.get("event_id") for e in events if e.get("event_id")]
        if not event_ids:
            print(f"  ⚠️  Race {race_id} ({race_name}): could not read event IDs, skipping")
            continue

        # Use the first event's start_time for the event year; fall back to race-level date
        first_event = events[0]
        event_name = first_event.get("name", race_name)
        event_year = _extract_year(first_event.get("start_time", ""))

        print(f"  Race {race_id}: {race_name} — {len(event_ids)} event(s), year {event_year or 'unknown'}")

        try:
            participants = _get_participants_for_race(race_id, event_ids)
            print(f"    {len(participants)} participants")
        except requests.HTTPError as exc:
            print(f"  ❌ HTTP error fetching race {race_id}: {exc}")
            continue
        except Exception as exc:
            print(f"  ❌ Unexpected error fetching race {race_id}: {exc}")
            continue

        for p in participants:
            all_rows.append(_participant_to_row(p, race_name, event_name, event_year))

        time.sleep(0.3)

    print(f"\n  Total rows: {len(all_rows)}")

    if not all_rows:
        print(f"  ⚠️  No data to upload for partner {partner_id}")
        return 0

    # Build CSV in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_HEADERS)
    writer.writeheader()
    writer.writerows(all_rows)

    filename = f"runsignup_export_{partner_id}_{datetime.now().strftime('%Y-%m-%d')}.csv"
    _upload_csv(drive_service, folder_id, filename, output.getvalue())

    return len(all_rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    missing = []
    if not API_KEY:
        missing.append("RUNSIGNUP_API_KEY")
    if not API_SECRET:
        missing.append("RUNSIGNUP_API_SECRET")
    if not PARTNER_IDS:
        missing.append("RUNSIGNUP_PARTNER_IDS")
    if not GDRIVE_CREDENTIALS:
        missing.append("GDRIVE_CREDENTIALS")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")

    drive_service = _get_drive_service()

    total_rows = 0
    errors = []

    for partner_id in PARTNER_IDS:
        folder_id = os.getenv(f"GDRIVE_FOLDER_ID_{partner_id}", "").strip()
        if not folder_id:
            print(f"⚠️  GDRIVE_FOLDER_ID_{partner_id} not set — skipping partner {partner_id}")
            errors.append(partner_id)
            continue
        try:
            rows = _fetch_partner(partner_id, folder_id, drive_service)
            total_rows += rows
        except Exception as exc:
            print(f"❌ Failed to process partner {partner_id}: {exc}")
            errors.append(partner_id)

    print(f"\n{'='*50}")
    print(f"Done. Total rows uploaded: {total_rows}")
    if errors:
        print(f"Partners with errors: {', '.join(errors)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
