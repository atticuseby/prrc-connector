#!/usr/bin/env python3
"""
Quick script to check what customer names are in the RICS CSV file.
Helps debug name filtering issues.
"""

import sys
import csv
from collections import Counter

def check_names(csv_path, search_term=None):
    """Check customer names in CSV file."""
    if not csv_path:
        csv_path = "rics_customer_purchase_history_deduped.csv"
    
    print(f"📄 Checking CSV: {csv_path}")
    print()
    
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            names = []
            for row in reader:
                name = row.get("CustomerName", "").strip()
                if name:
                    names.append(name)
        
        if not names:
            print("⚠️  No customer names found in CSV!")
            return
        
        # Count unique names
        name_counts = Counter(names)
        unique_names = len(name_counts)
        total_rows = len(names)
        
        print(f"📊 Found {total_rows} rows with customer names")
        print(f"📊 Found {unique_names} unique customer names")
        print()
        
        # If search term provided, show matches
        if search_term:
            print(f"🔍 Searching for names containing: '{search_term}'")
            print()
            matches = [name for name in names if search_term.lower() in name.lower()]
            if matches:
                match_counts = Counter(matches)
                print(f"✅ Found {len(matches)} matching rows:")
                for name, count in match_counts.most_common():
                    print(f"   '{name}' ({count} rows)")
            else:
                print(f"❌ No matches found for '{search_term}'")
            print()
        
        # Show all unique names (or first 50 if too many)
        print("📋 All unique customer names in CSV:")
        if unique_names > 50:
            print(f"   (Showing first 50 of {unique_names} unique names)")
            for name, count in name_counts.most_common(50):
                print(f"   '{name}' ({count} rows)")
        else:
            for name, count in name_counts.most_common():
                print(f"   '{name}' ({count} rows)")
        
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_path}")
        print("💡 Make sure you're running this from the project root")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")

if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "rics_customer_purchase_history_deduped.csv"
    search_term = sys.argv[2] if len(sys.argv) > 2 else None
    
    check_names(csv_path, search_term)

