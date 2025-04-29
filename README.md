# RICS Connector

This project pulls daily in-store transaction data from RICS and sends it to Optimizely for segmentation and automation.

## Folder Structure

- `scripts/` – contains the sync script and helper functions
- `sample_payloads/` – sample data payloads to test formatting
- `logs/` – output or error logs
- `.env.example` – a sample env file with expected variables

## How to Use

1. Copy `.env.example` to `.env` and fill in your keys
2. Run `sync_to_optimizely.py` manually or on a daily schedule
3. Check `logs/` for success or failure

## Variables Required

- `OPTIMIZELY_API_KEY`
- `RICS_API_KEY` (when provided)
