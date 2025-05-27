import os
import csv
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# === CONFIG ===
RICS_API_TOKEN = os.getenv("RICS_API_TOKEN")
OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN")
TEST_EMAIL = os.getenv("TEST_EMAIL", "test@example.com")
GDRIVE_FOLDER_ID_RICS = os.getenv("GDRIVE_FOLDER_ID_RICS")
CREDS_PATH = "optimizely_connector/service_account.json"
OUTPUT_DIR = "optimizely_connector/output"
DATA_DIR = "data"
BATCH_SIZE = 500
STORE_CODE = 12132
MAX_SKIP = 50000  # Production volume

# Detect if we're in the test branch
IS_TEST_BRANCH = os.getenv("GITHUB_REF", "").endswith("/test")

# === SETUP ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

def log(msg): print(f"[log] {msg}")

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    return build("drive", "v3", credentials=creds)

def upload_to_drive(filepath):
    service = get_drive_service()
    file_metadata = {
        "name": os.path.basename(filepath),
        "parents": [GDRIVE_FOLDER_ID_RICS]
    }
    media = MediaFileUpload(filepath, mimetype="text/csv")
    file = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    log(f"✅ Uploaded to Drive as file ID: {file.get('id')}")

def fetch_rics_data():
    all_rows = []
    skip = 0

    while skip < MAX_SKIP:
        log(f"📦 Fetching customers from skip {skip}")
        try:
            res = requests.post(
                url="https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                headers={"Authorization": f"Bearer {RICS_API_TOKEN}"},
                json={"StoreCode": STORE_CODE, "Skip": skip, "Take": 100},
                timeout=20
            )
            res.raise_for_status()
            customers = res.json().get("Customers", [])
        except Exception as e:
            log(f"❌ Failed to fetch RICS data: {e}")
            break

        if not customers:
            break

        for c in customers:
            mailing = c.get("MailingAddress", {})
            all_rows.append({
                "rics_id": c.get("CustomerId"),
                "email": c.get("Email", "").strip(),
                "first_name": c.get("FirstName", "").strip(),
                "last_name": c.get("LastName", "").strip(),
                "orders": c.get("OrderCount", 0),
                "total_spent": c.get("TotalSpent", 0),
                "city": mailing.get("City", "").strip(),
                "state": mailing.get("State", "").strip(),
                "zip": mailing.get("PostalCode", "").strip(),
                "phone": c.get("PhoneNumber", "").strip()
            })

        skip += 100

    if IS_TEST_BRANCH:
        log("🧪 Injecting 3 test contacts (test branch only)")
        all_rows.extend([
            {
                "rics_id": "test-001",
                "email": TEST_EMAIL,
                "first_name": "Test",
                "last_name": "Email",
                "orders": 1,
                "total_spent": 10,
                "city": "Testville",
                "state": "TN",
                "zip": "37201",
                "phone": ""
            },
            {
                "rics_id": "test-002",
                "email": "",
                "first_name": "Phone",
                "last_name": "Only",
                "orders": 0,
                "total_spent": 0,
                "city": "Franklin",
                "state": "TN",
                "zip": "37064",
                "phone": "5551234567"
            },
            {
                "rics_id": "test-003",
                "email": "test+both@bandit.com",
                "first_name": "Dual",
                "last_name": "Contact",
                "orders": 2,
                "total_spent": 200,
                "city": "Memphis",
                "state": "TN",
                "zip": "38103",
                "phone": "5559876543"
            }
        ])

    return all_rows

def push_to_optimizely(rows):
    batch = []
    for row in rows:
        email = row.get("email")
        phone = row.get("phone")
        rics_id = row.get("rics_id")

        if not email and not phone:
            continue

        identifiers = {}
        if email:
            identifiers["email"] = email
        if phone:
            identifiers["phone_number"] = phone
        if rics_id:
            identifiers["customer_id"] = rics_id

        props = {
            "first_name": row.get("first_name", ""),
            "last_name": row.get("last_name", ""),
            "city": row.get("city", ""),
            "state": row.get("state", ""),
            "zip": row.get("zip", ""),
            "orders": row.get("orders"),
            "total_spent": row.get("total_spent"),
            "rics_id": rics_id
        }

        if props["first_name"] and props["last_name"]:
            props["name"] = f"{props['first_name']} {props['last_name']}"

        props = {k: v for k, v in props.items() if v not in (None, "", "NULL")}

        batch.append({
            "type": "customer_update",
            "identifiers": identifiers,
            "properties": props
        })

        if len(batch) >= BATCH_SIZE:
            send_batch(batch)
            batch = []

    if batch:
        send_batch(batch)

def send_batch(batch):
    try:
        res = requests.post(
            "https://api.zaius.com/v3/events",
            headers={
                "x-api-key": OPTIMIZELY_API_TOKEN,
                "Content-Type": "application/json"
            },
            json=batch,
            timeout=15
        )
        if res.status_code in [200, 202]:
            log(f"✅ Batch sent ({len(batch)} records)")
        else:
            log(f"❌ Batch failed: {res.status_code} — {res.text}")
    except Exception as e:
        log(f"❌ Network error: {e}")

def save_csv(rows, filename):
    fields = ["rics_id", "email", "first_name", "last_name", "orders", "total_spent", "city", "state", "zip", "phone"]
    paths = [
        os.path.join(OUTPUT_DIR, filename),
        os.path.join(DATA_DIR, filename)
    ]
    for path in paths:
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        log(f"📁 Saved CSV to: {path}")
    return os.path.join(OUTPUT_DIR, filename)

def main():
    log("🚀 Starting real-time RICS → Optimizely sync")
    rows = fetch_rics_data()
    log(f"📊 Fetched {len(rows)} customers from RICS")

    push_to_optimizely(rows)

    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_export_{timestamp}.csv"
    csv_path = save_csv(rows, filename)

    upload_to_drive(csv_path)
    log("✅ All done!")

if __name__ == "__main__":
    main()
