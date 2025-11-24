#!/usr/bin/env python3
"""
Helper script to verify RunSignup GitHub Secrets are configured correctly.

This script helps identify:
1. Which Google Drive folder IDs are actually being used
2. Whether Optimizely list IDs are correctly mapped
3. If the folder IDs can be accessed

Usage:
    python scripts/verify_runsignup_secrets.py
"""

import os
import json
import sys
from googleapiclient.discovery import build
from google.oauth2 import service_account

def get_drive_service():
    """Initialize Google Drive service."""
    creds_json = os.getenv("GDRIVE_CREDENTIALS")
    if not creds_json:
        print("❌ GDRIVE_CREDENTIALS not set")
        return None
    
    try:
        creds_info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            creds_info,
            scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        print(f"❌ Error creating Drive service: {e}")
        return None

def verify_folder_id(drive_service, folder_id, partner_id):
    """Verify a Google Drive folder ID and return folder info."""
    if not folder_id:
        return None, "NOT SET"
    
    # Google Drive folder IDs are typically long alphanumeric strings
    # Optimizely list IDs are typically shorter with underscores/hyphens
    if len(folder_id) < 20 or not folder_id.replace("-", "").replace("_", "").isalnum():
        return None, f"⚠️  Looks like an Optimizely list ID, not a Google Drive folder ID (too short/contains underscores)"
    
    try:
        folder_info = drive_service.files().get(
            fileId=folder_id,
            fields="id,name,webViewLink",
            supportsAllDrives=True
        ).execute()
        return folder_info, "✅ Valid"
    except Exception as e:
        return None, f"❌ Error: {str(e)[:100]}"

def main():
    print("🔍 Verifying RunSignup GitHub Secrets Configuration\n")
    
    # Check required secrets
    required = ["OPTIMIZELY_API_TOKEN", "GDRIVE_CREDENTIALS", "RSU_FOLDER_IDS"]
    missing = [r for r in required if not os.getenv(r)]
    if missing:
        print(f"❌ Missing required secrets: {', '.join(missing)}")
        sys.exit(1)
    
    # Parse partner IDs
    rsu_raw = os.getenv("RSU_FOLDER_IDS", "").strip()
    raw_ids = [pid.strip() for pid in rsu_raw.split(",") if pid.strip()]
    enabled_partner_ids = []
    for raw_id in raw_ids:
        partner_id = raw_id.replace("id_", "").replace("ID_", "").strip()
        if partner_id:
            enabled_partner_ids.append(partner_id)
    
    print(f"📋 Enabled partner IDs: {', '.join(enabled_partner_ids)}\n")
    
    # Initialize Drive service
    drive_service = get_drive_service()
    if not drive_service:
        print("⚠️  Cannot verify Google Drive folder IDs without Drive service")
        print("   But we can still check the configuration...\n")
    
    # Check each partner's configuration
    print("=" * 70)
    print("PARTNER CONFIGURATION CHECK")
    print("=" * 70)
    
    for partner_id in ["1384", "1385", "1411"]:
        print(f"\n📌 Partner {partner_id}:")
        
        # Check Google Drive folder ID
        gdrive_key = f"GDRIVE_FOLDER_ID_{partner_id}"
        gdrive_value = os.getenv(gdrive_key, "").strip()
        
        print(f"   {gdrive_key}:")
        if not gdrive_value:
            print(f"      ❌ NOT SET")
        else:
            # Mask for security
            masked = gdrive_value[:6] + "..." + gdrive_value[-6:] if len(gdrive_value) > 12 else gdrive_value
            print(f"      Value: {masked} (length: {len(gdrive_value)})")
            
            if drive_service:
                folder_info, status = verify_folder_id(drive_service, gdrive_value, partner_id)
                print(f"      Status: {status}")
                if folder_info:
                    print(f"      Folder Name: {folder_info.get('name', 'N/A')}")
                    print(f"      Folder URL: {folder_info.get('webViewLink', 'N/A')}")
            else:
                # Basic validation without Drive service
                if len(gdrive_value) < 20:
                    print(f"      ⚠️  WARNING: This looks like an Optimizely list ID, not a Google Drive folder ID!")
        
        # Check Optimizely list ID
        optimizely_key = f"OPTIMIZELY_LIST_ID_{partner_id}"
        optimizely_value = os.getenv(optimizely_key, "").strip()
        
        print(f"   {optimizely_key}:")
        if not optimizely_value:
            print(f"      ❌ NOT SET")
        else:
            print(f"      Value: {optimizely_value}")
            # Optimizely list IDs are typically shorter with underscores/hyphens
            if len(optimizely_value) > 30:
                print(f"      ⚠️  WARNING: This looks like a Google Drive folder ID, not an Optimizely list ID!")
        
        # Check if values are swapped
        if gdrive_value and optimizely_value:
            if len(gdrive_value) < 20 and len(optimizely_value) > 20:
                print(f"      ⚠️  ⚠️  ⚠️  VALUES APPEAR TO BE SWAPPED! ⚠️  ⚠️  ⚠️")
                print(f"      The {gdrive_key} looks like an Optimizely list ID")
                print(f"      The {optimizely_key} looks like a Google Drive folder ID")
                print(f"      You should swap these values in GitHub Secrets!")
    
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    
    # Check if all enabled partners have both values set
    all_configured = True
    for partner_id in enabled_partner_ids:
        gdrive_key = f"GDRIVE_FOLDER_ID_{partner_id}"
        optimizely_key = f"OPTIMIZELY_LIST_ID_{partner_id}"
        
        gdrive_value = os.getenv(gdrive_key, "").strip()
        optimizely_value = os.getenv(optimizely_key, "").strip()
        
        if not gdrive_value or not optimizely_value:
            print(f"❌ Partner {partner_id} is missing configuration")
            all_configured = False
        else:
            print(f"✅ Partner {partner_id} has both values set")
    
    if all_configured:
        print("\n✅ All enabled partners are configured!")
    else:
        print("\n⚠️  Some partners are missing configuration")
    
    print("\n💡 To find a Google Drive folder ID:")
    print("   1. Open the folder in Google Drive")
    print("   2. Look at the URL: https://drive.google.com/drive/folders/FOLDER_ID_HERE")
    print("   3. Copy the FOLDER_ID_HERE part (long alphanumeric string)")

if __name__ == "__main__":
    main()


