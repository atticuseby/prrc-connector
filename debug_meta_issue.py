#!/usr/bin/env python3
"""
Debug Meta Issue

This script helps diagnose why offline conversions aren't appearing in Ads Manager.
"""

import os
import json
import csv
from datetime import datetime, timezone, timedelta

def check_csv_data():
    """Check the CSV data we have."""
    print("🔍 CHECKING CSV DATA")
    print("=" * 50)
    
    csv_file = "rics_customer_purchase_history_deduped.csv"
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return False
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    print(f"✅ CSV file found: {csv_file}")
    print(f"📊 Total rows: {len(rows)}")
    
    if len(rows) == 0:
        print("❌ No data rows in CSV")
        return False
    
    # Check first few rows
    print(f"\n🔍 First 3 rows:")
    for i, row in enumerate(rows[:3]):
        print(f"  Row {i+1}:")
        print(f"    TicketNumber: {row.get('TicketNumber')}")
        print(f"    SaleDateTime: {row.get('SaleDateTime')}")
        print(f"    CustomerEmail: {row.get('CustomerEmail')}")
        print(f"    CustomerName: {row.get('CustomerName')}")
        print(f"    AmountPaid: {row.get('AmountPaid')}")
    
    # Check date range
    dates = []
    for row in rows:
        sale_dt = row.get('SaleDateTime')
        if sale_dt:
            try:
                dt = datetime.fromisoformat(sale_dt.replace('Z', '+00:00'))
                dates.append(dt)
            except:
                pass
    
    if dates:
        min_date = min(dates)
        max_date = max(dates)
        print(f"\n📅 Date range: {min_date} to {max_date}")
        
        # Check if dates are within Meta's 7-day window
        cutoff = datetime.now(timezone.utc) - timedelta(days=7)
        recent_dates = [d for d in dates if d.replace(tzinfo=timezone.utc) > cutoff]
        print(f"📅 Recent dates (last 7 days): {len(recent_dates)}")
        
        if len(recent_dates) == 0:
            print("⚠️ WARNING: No recent dates within Meta's 7-day window!")
            print("🔧 This could be why events aren't appearing in Ads Manager")
            return False
    
    return True

def check_meta_credentials():
    """Check if Meta credentials are available."""
    print("\n🔍 CHECKING META CREDENTIALS")
    print("=" * 50)
    
    access_token = os.getenv("META_ACCESS_TOKEN")
    dataset_id = os.getenv("META_DATASET_ID")
    
    print(f"Access token: {'✅ Set' if access_token else '❌ Not set'}")
    print(f"Dataset ID: {'✅ Set' if dataset_id else '❌ Not set'}")
    
    if access_token:
        print(f"Token length: {len(access_token)}")
    if dataset_id:
        print(f"Dataset ID: {dataset_id}")
    
    return bool(access_token and dataset_id)

def simulate_event_building():
    """Simulate building events to see what would be sent."""
    print("\n🔍 SIMULATING EVENT BUILDING")
    print("=" * 50)
    
    csv_file = "rics_customer_purchase_history_deduped.csv"
    if not os.path.exists(csv_file):
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    if not rows:
        print("❌ No data to process")
        return
    
    # Process first few rows
    events = []
    cutoff_time = datetime.utcnow() - timedelta(days=7)
    
    for i, row in enumerate(rows[:5]):  # Process first 5 rows
        ticket_no = row.get('TicketNumber', '').strip()
        if not ticket_no:
            continue
        
        sale_dt_str = row.get('SaleDateTime') or row.get('TicketDateTime')
        if not sale_dt_str:
            continue
        
        # Parse date
        try:
            dt = datetime.fromisoformat(sale_dt_str.replace('Z', '+00:00'))
            event_time = int(dt.timestamp())
        except:
            print(f"⚠️ Could not parse date: {sale_dt_str}")
            continue
        
        # Check if within Meta's 7-day window
        if dt < cutoff_time:
            print(f"⚠️ Row {i+1}: Date {dt} is too old (outside 7-day window)")
            continue
        
        # Build event
        event = {
            "event_name": "Purchase",
            "event_time": event_time,
            "action_source": "offline",
            "event_id": f"purchase-{ticket_no}-{event_time}",
            "user_data": {
                "em": row.get('CustomerEmail', '').strip().lower() if row.get('CustomerEmail') else None,
                "fn": row.get('CustomerName', '').split()[0].lower() if row.get('CustomerName') else None,
            },
            "custom_data": {
                "order_id": str(ticket_no),
                "value": float(row.get('AmountPaid', 0)),
                "currency": "USD"
            }
        }
        
        # Remove None values
        event["user_data"] = {k: v for k, v in event["user_data"].items() if v}
        
        if not event["user_data"]:
            print(f"⚠️ Row {i+1}: No user data available")
            continue
        
        events.append(event)
        print(f"✅ Row {i+1}: Built event for ticket {ticket_no}")
        print(f"   Event time: {datetime.fromtimestamp(event_time)}")
        print(f"   User data: {event['user_data']}")
        print(f"   Value: ${event['custom_data']['value']}")
    
    print(f"\n📊 Total events that would be sent: {len(events)}")
    
    if len(events) == 0:
        print("❌ No events would be sent!")
        print("🔧 Possible reasons:")
        print("  - All dates are too old (outside 7-day window)")
        print("  - Missing customer email/name data")
        print("  - Invalid date formats")
    else:
        print("✅ Events look good for sending to Meta")

def main():
    print("🚀 DEBUG META ISSUE")
    print("=" * 50)
    print(f"Time: {datetime.now()}")
    
    # Check CSV data
    csv_ok = check_csv_data()
    
    # Check credentials
    creds_ok = check_meta_credentials()
    
    # Simulate event building
    simulate_event_building()
    
    print(f"\n📋 SUMMARY")
    print("=" * 50)
    print(f"CSV data: {'✅ OK' if csv_ok else '❌ Issues'}")
    print(f"Credentials: {'✅ OK' if creds_ok else '❌ Missing'}")
    
    if not csv_ok:
        print("\n🔧 FIX CSV ISSUES FIRST")
        print("  - Run: python3 scripts/sync_rics_live.py --debug")
        print("  - Check that recent data is being fetched")
    
    if not creds_ok:
        print("\n🔧 SET META CREDENTIALS")
        print("  - export META_ACCESS_TOKEN='your_token'")
        print("  - export META_DATASET_ID='855183627077424'")
    
    if csv_ok and creds_ok:
        print("\n🔧 TEST META SYNC")
        print("  - python3 scripts/sync_rics_to_meta.py")
        print("  - python3 meta_send_test_event.py")

if __name__ == "__main__":
    main()
