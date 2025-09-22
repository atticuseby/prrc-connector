#!/usr/bin/env python3
"""
Test the fixed RICS system to see if it now pulls data correctly.
"""

import os
import sys
from datetime import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

def test_fixed_system():
    """Test the fixed RICS data fetching system."""
    
    print("🧪 Testing Fixed RICS System")
    print("=" * 50)
    
    # Check if token is set
    token = os.getenv("RICS_API_TOKEN")
    if not token or token == "your_rics_api_token_here":
        print("❌ RICS_API_TOKEN not set!")
        print("\nTo set it, run:")
        print("export RICS_API_TOKEN='your_actual_token_here'")
        print("\nOr edit the .env file with your actual token.")
        return False
    
    print(f"✅ RICS_API_TOKEN is set (length: {len(token)})")
    
    try:
        # Import and run the fixed fetch function
        from rics_connector.fetch_rics_data import fetch_rics_data_with_purchase_history
        from scripts.helpers import log_message
        
        print("\n📥 Running RICS data fetch with fixed date filtering...")
        print("   - Extended API range: 14 days")
        print("   - Extended cutoff: 30 days") 
        print("   - Added debug logging for filtered dates")
        
        # Run the fetch
        output_path = fetch_rics_data_with_purchase_history()
        
        print(f"\n✅ Fetch completed!")
        print(f"📁 Output file: {output_path}")
        
        # Check if file has data
        if os.path.exists(output_path):
            with open(output_path, 'r') as f:
                lines = f.readlines()
            
            print(f"📊 File contains {len(lines)} lines")
            if len(lines) > 1:  # More than just header
                print("🎉 SUCCESS! File contains data (not empty)")
                print(f"   Sample line: {lines[1][:100]}...")
            else:
                print("⚠️ File only contains headers - still no data")
                print("   This might indicate:")
                print("   - No sales in the last 14 days")
                print("   - API authentication issues")
                print("   - Different store codes needed")
        else:
            print("❌ Output file was not created")
            
    except Exception as e:
        print(f"❌ Error running fetch: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = test_fixed_system()
    
    if success:
        print("\n🎯 Next steps:")
        print("1. Check the generated CSV file for data")
        print("2. If data is present, run the full sync:")
        print("   python3 scripts/sync_rics_live.py")
        print("3. Verify data uploads to Google Drive")
    else:
        print("\n🔧 Troubleshooting:")
        print("1. Make sure RICS_API_TOKEN is set correctly")
        print("2. Check if your RICS account has recent sales data")
        print("3. Verify the API endpoint is accessible")

