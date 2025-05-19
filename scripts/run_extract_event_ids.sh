#!/bin/bash

set -e  # Stop on error

echo "🚀 Starting Extract + Upload"

python optimizely_connector/extract_event_ids.py
python optimizely_connector/upload_to_gdrive.py

echo "✅ Done"
