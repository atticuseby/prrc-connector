# Optimizely Data Connector

This project automates the process of pulling customer data from two external sources — **RICS** and **RunSignUp** — and syncing it with **Optimizely (Zaius)** for segmentation and email automation. It also uploads exported data to **Google Drive** for backup and visibility.

---

## 🧱 Folder Structure

project-root/
├── optimizely_connector/ # Main orchestrator scripts
│ ├── main.py # Runs the RICS pipeline
│ └── output/ # Exported CSVs from both sources
├── rics_connector/ # RICS data logic
│ ├── fetch_rics_data.py
│ ├── sync_rics_to_optimizely.py
├── runsignup_connector/ # RunSignUp data logic
│ ├── extract_event_ids.py
│ ├── run_signup_to_optimizely.py
│ ├── main_runsignup.py
├── scripts/ # Shared utilities
│ ├── upload_to_gdrive.py
│ ├── config.py
│ └── helpers.py
├── .github/workflows/ # GitHub Action workflows
│ └── run_runsignup_connector.yml
├── .env.example # Sample environment variable file

yaml
Copy
Edit

---

## 🚀 How It Works

### ✅ RICS Flow
1. Pulls daily in-store customer transaction data
2. Exports CSV to `output/`
3. Uploads to Google Drive (`/RICS/`)
4. Pushes to Optimizely via API

Run: `python optimizely_connector/main.py`

---

### ✅ RunSignUp Flow
1. Extracts race + event IDs
2. Pulls registrant data
3. Exports CSV to `output/`
4. Uploads to Google Drive (`/RunSignUp/`)
5. Pushes to Optimizely via API

Run manually: `python runsignup_connector/main_runsignup.py`  
Or via GitHub Actions: **Actions → Run RunSignUp Connector**

---

## 🔐 Environment Variables Required

These should be added to your local `.env` file or GitHub Secrets:

| Variable                  | Purpose                                |
|---------------------------|----------------------------------------|
| `OPTIMIZELY_API_TOKEN`    | API token for Optimizely/Zaius         |
| `RUNSIGNUP_API_KEY`       | Partner API key from RunSignUp         |
| `RUNSIGNUP_API_SECRET`    | Partner API secret from RunSignUp      |
| `RICS_API_KEY` *(if used)*| API key for RICS (if using their API)  |
| `GDRIVE_FOLDER_ID`        | Google Drive shared folder ID          |
| `GDRIVE_CREDENTIALS`      | Full JSON string from service account  |

---

## ⚙️ Setup

1. Copy the example env file:
   ```bash
   cp .env.example .env
Add your API keys and credentials

Install dependencies:

bash
Copy
Edit
pip install -r requirements.txt
Run manually or through GitHub Actions

📝 Notes
RunSignUp API uses api_key in URL and X-RSU-API-SECRET header

Data is backed up daily to Google Drive subfolders

Only valid customer records with emails are pushed to Optimizely

📅 Automation
RunSignUp connector can be triggered manually via GitHub Actions

RICS connector is designed for daily or scheduled execution via cron, Replit, or GitHub Actions
