#!/bin/bash

echo "🔧 PRRC Connector Local Test Setup"
echo "=================================="

# Check if we're in the right directory
if [ ! -f "scripts/sync_rics_live.py" ]; then
    echo "❌ Please run this script from the project root directory"
    exit 1
fi

echo "✅ Found sync script"

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo "✅ Found virtual environment"
    echo "🔧 Activating virtual environment..."
    source venv/bin/activate
else
    echo "⚠️ No virtual environment found"
    echo "🔧 Creating virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    echo "🔧 Installing requirements..."
    pip install -r requirements.txt
fi

echo ""
echo "🔧 Environment setup complete!"
echo ""
echo "📋 NEXT STEPS:"
echo "1. Set your RICS_API_TOKEN:"
echo "   export RICS_API_TOKEN='your_token_here'"
echo ""
echo "2. Set your Meta credentials (optional for testing):"
echo "   export META_ACCESS_TOKEN='your_meta_token'"
echo "   export META_DATASET_ID='your_dataset_id'"
echo "   export TEST_EMAIL='test@example.com'"
echo ""
echo "3. Run the diagnostic:"
echo "   python3 scripts/debug_rics_data_flow.py"
echo ""
echo "4. Run the sync with debug:"
echo "   python3 scripts/sync_rics_live.py --debug"
echo ""
echo "5. Run without deduplication:"
echo "   python3 scripts/sync_rics_live.py --no-dedup --debug"
echo ""

# Check if token is already set
if [ -n "$RICS_API_TOKEN" ]; then
    echo "✅ RICS_API_TOKEN is already set"
    echo "🚀 Running diagnostic now..."
    python3 scripts/debug_rics_data_flow.py
else
    echo "⚠️ RICS_API_TOKEN not set - please set it first"
fi
