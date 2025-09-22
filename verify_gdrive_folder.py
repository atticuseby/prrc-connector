#!/usr/bin/env python3
"""
Script to verify Google Drive folder ID and list contents
"""
import os
import json
from googleapiclient.discovery import build
from google.oauth2 import service_account

def verify_gdrive_folder():
    # Get credentials from environment
    creds_json = os.getenv("GDRIVE_CREDENTIALS")
    folder_id = os.getenv("GDRIVE_FOLDER_ID_RICS")
    
    if not creds_json:
        print("❌ GDRIVE_CREDENTIALS not set in environment")
        print("Set it with: export GDRIVE_CREDENTIALS='your_json_here'")
        return
    
    if not folder_id:
        print("❌ GDRIVE_FOLDER_ID_RICS not set in environment")
        print("Set it with: export GDRIVE_FOLDER_ID_RICS='your_folder_id'")
        return
    
    try:
        # Parse credentials
        creds_info = json.loads(creds_json)
        service_account_email = creds_info.get('client_email', 'Unknown')
        print(f"📧 Service Account Email: {service_account_email}")
        
        # Create credentials and service
        creds = service_account.Credentials.from_service_account_info(
            creds_info, 
            scopes=["https://www.googleapis.com/auth/drive"]
        )
        service = build("drive", "v3", credentials=creds)
        
        print(f"📁 Checking folder ID: {folder_id}")
        
        # Get folder info
        try:
            folder_info = service.files().get(
                fileId=folder_id,
                fields="id,name,webViewLink,permissions",
                supportsAllDrives=True
            ).execute()
            
            print(f"✅ Folder found: {folder_info.get('name')}")
            print(f"🔗 Folder URL: {folder_info.get('webViewLink')}")
            
            # List files in folder
            print(f"\n📋 Files in folder '{folder_info.get('name')}':")
            results = service.files().list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="files(id,name,createdTime,modifiedTime,size)",
                supportsAllDrives=True
            ).execute()
            
            files = results.get('files', [])
            if files:
                for file in files:
                    print(f"  📄 {file.get('name')} (ID: {file.get('id')})")
                    print(f"      Created: {file.get('createdTime')}")
                    print(f"      Modified: {file.get('modifiedTime')}")
                    print(f"      Size: {file.get('size', 'Unknown')} bytes")
                    print()
            else:
                print("  (No files found)")
                
        except Exception as e:
            print(f"❌ Error accessing folder: {e}")
            print("This might mean:")
            print("1. Folder ID is incorrect")
            print("2. Service account doesn't have access to the folder")
            print("3. Folder is not shared with the service account")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    verify_gdrive_folder()
