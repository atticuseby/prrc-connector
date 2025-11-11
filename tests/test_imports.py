"""
Sanity test for RunSignup connector imports.

This test verifies that all required modules can be imported successfully.
Run this before the main sync job to catch import errors early.
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_imports():
    """Test that all required modules can be imported."""
    errors = []
    
    # Test runsignup_connector imports
    try:
        import runsignup_connector.main_runsignup
        print("✅ runsignup_connector.main_runsignup")
    except ImportError as e:
        errors.append(f"runsignup_connector.main_runsignup: {e}")
        print(f"❌ runsignup_connector.main_runsignup: {e}")
    
    try:
        import runsignup_connector.optimizely_client
        print("✅ runsignup_connector.optimizely_client")
    except ImportError as e:
        errors.append(f"runsignup_connector.optimizely_client: {e}")
        print(f"❌ runsignup_connector.optimizely_client: {e}")
    
    # Test scripts imports
    try:
        from scripts.process_runsignup_csvs import process_runsignup_csvs
        print("✅ scripts.process_runsignup_csvs")
    except ImportError as e:
        errors.append(f"scripts.process_runsignup_csvs: {e}")
        print(f"❌ scripts.process_runsignup_csvs: {e}")
    
    # Test Optimizely client functions
    try:
        from runsignup_connector.optimizely_client import post_profile, post_event
        print("✅ post_profile, post_event functions")
    except ImportError as e:
        errors.append(f"Optimizely client functions: {e}")
        print(f"❌ Optimizely client functions: {e}")
    
    if errors:
        print(f"\n❌ Import test failed with {len(errors)} error(s)")
        sys.exit(1)
    else:
        print("\n✅ All imports successful")


if __name__ == "__main__":
    test_imports()

