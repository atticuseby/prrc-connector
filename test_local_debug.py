#!/usr/bin/env python3
"""
Local test script for PRRC connector debugging

Run this locally to test the connector without needing GitHub Actions
"""

import os
import sys
import subprocess
from datetime import datetime

def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"\n🔧 {description}")
    print(f"Command: {cmd}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(f"Exit code: {result.returncode}")
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        return result.returncode == 0
    except Exception as e:
        print(f"Error running command: {e}")
        return False

def main():
    print("🚀 PRRC CONNECTOR LOCAL DEBUG TEST")
    print("=" * 60)
    print(f"Timestamp: {datetime.now()}")
    
    # Check if we're in the right directory
    if not os.path.exists("scripts/sync_rics_live.py"):
        print("❌ Please run this script from the project root directory")
        return 1
    
    # Test 1: RICS Token Diagnostic
    print("\n" + "=" * 60)
    print("TEST 1: RICS TOKEN DIAGNOSTIC")
    print("=" * 60)
    
    success1 = run_command(
        "python scripts/diagnose_rics_token.py",
        "Testing RICS API token"
    )
    
    if not success1:
        print("❌ RICS token test failed - check your RICS_API_TOKEN")
        print("🔧 Set RICS_API_TOKEN environment variable and try again")
        return 1
    
    # Test 2: Meta Offline Events Test
    print("\n" + "=" * 60)
    print("TEST 2: META OFFLINE EVENTS TEST")
    print("=" * 60)
    
    success2 = run_command(
        "python scripts/test_meta_offline_events.py",
        "Testing Meta offline events API"
    )
    
    if not success2:
        print("❌ Meta test failed - check your Meta credentials")
        print("🔧 Set META_ACCESS_TOKEN, META_DATASET_ID, and TEST_EMAIL")
        return 1
    
    # Test 3: RICS Sync with Debug
    print("\n" + "=" * 60)
    print("TEST 3: RICS SYNC WITH DEBUG")
    print("=" * 60)
    
    success3 = run_command(
        "python scripts/sync_rics_live.py --debug",
        "Running RICS sync with debug counters"
    )
    
    if not success3:
        print("❌ RICS sync failed")
        return 1
    
    # Test 4: RICS Sync without Dedup
    print("\n" + "=" * 60)
    print("TEST 4: RICS SYNC WITHOUT DEDUP")
    print("=" * 60)
    
    success4 = run_command(
        "python scripts/sync_rics_live.py --no-dedup --debug",
        "Running RICS sync without deduplication"
    )
    
    if not success4:
        print("❌ RICS sync without dedup failed")
        return 1
    
    # Check output files
    print("\n" + "=" * 60)
    print("OUTPUT FILE ANALYSIS")
    print("=" * 60)
    
    output_dir = "optimizely_connector/output"
    if os.path.exists(output_dir):
        files = os.listdir(output_dir)
        csv_files = [f for f in files if f.endswith('.csv')]
        
        print(f"📁 Found {len(csv_files)} CSV files in output directory:")
        for file in sorted(csv_files):
            file_path = os.path.join(output_dir, file)
            size = os.path.getsize(file_path)
            print(f"  {file}: {size} bytes")
            
            # Count lines
            try:
                with open(file_path, 'r') as f:
                    lines = f.readlines()
                    print(f"    Lines: {len(lines)} (including header)")
                    if len(lines) > 1:
                        print(f"    Data rows: {len(lines) - 1}")
                    else:
                        print("    ⚠️ No data rows!")
            except Exception as e:
                print(f"    Error reading file: {e}")
    else:
        print("❌ Output directory not found")
    
    # Check logs
    print("\n" + "=" * 60)
    print("LOG ANALYSIS")
    print("=" * 60)
    
    if os.path.exists("logs/sync_log.txt"):
        print("📄 Recent log entries:")
        try:
            result = subprocess.run(
                "tail -20 logs/sync_log.txt",
                shell=True,
                capture_output=True,
                text=True
            )
            print(result.stdout)
        except Exception as e:
            print(f"Error reading logs: {e}")
    else:
        print("❌ No sync log found")
    
    # Final summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    tests = [
        ("RICS Token", success1),
        ("Meta Events", success2),
        ("RICS Sync", success3),
        ("RICS No-Dedup", success4)
    ]
    
    all_passed = True
    for test_name, passed in tests:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name:15} {status}")
        if not passed:
            all_passed = False
    
    if all_passed:
        print("\n🎉 ALL TESTS PASSED!")
        print("🔧 The connector should work in GitHub Actions")
    else:
        print("\n⚠️ SOME TESTS FAILED")
        print("🔧 Fix the failing tests before running in production")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
