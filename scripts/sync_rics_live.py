import os
import csv
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

RICS_API_TOKEN = os.getenv("RICS_API_TOKEN")
OPTIMIZELY_API_TOKEN = os.getenv("OPTIMIZELY_API_TOKEN")
TEST_EMAIL = os.getenv("TEST_EMAIL", "test@example.com")
GDRIVE_FOLDER_ID_RICS = os.getenv("GDRIVE_FOLDER_ID_RICS")
CREDS_PATH = "optimizely_connector/service_account.json"
OUTPUT_DIR = "optimizely_connector/output"
DATA_DIR = "data"
BATCH_SIZE = 500
STORE_CODE = 12132
MAX_SKIP = float("inf")
IS_TEST_BRANCH = os.getenv("GITHUB_REF", "").endswith("/test")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)

def log(msg): print(f"[log] {msg}", flush=True)

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

def log_customer(c, index):
    log(f"👤 Customer {index}: {c['first_name']} {c['last_name']} | "
        f"Email: {c['email'] or '—'} | Phone: {c['phone'] or '—'} | "
        f"Orders: {c['orders']} | Spent: ${c['total_spent']} | "
        f"{c['city']}, {c['state']} {c['zip']}")

def fetch_rics_data():
    all_rows = []
    skip = 0

    while True:
        log(f"📦 Fetching customers from skip {skip}")
        try:
            log(f"📡 Preparing RICS request for skip {skip}")
            res = requests.post(
                url="https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                headers={"Token": RICS_API_TOKEN},
                json={"StoreCode": STORE_CODE, "Skip": skip, "Take": 100},
                timeout=20
            )
            log(f"✅ RICS response status: {res.status_code}")
            res.raise_for_status()
            customers = res.json().get("Customers", [])
        except requests.exceptions.HTTPError as e:
            if res.status_code >= 500:
                log(f"❌ Server error on skip {skip}: {res.status_code} — stopping sync early")
                break
            else:
                log(f"❌ Client error on skip {skip}: {res.status_code} — skipping ahead")
                skip += 100
                continue
        except Exception as e:
            log(f"❌ Unexpected error on skip {skip}: {e}")
            break

        if not customers:
            log(f"📭 No customers returned at skip {skip} — ending")
            break

        if skip == 0 and customers:
            log(f"📄 RAW sample record:\n{customers[0]}")

        for i, c in enumerate(customers, start=1):
            mailing = c.get("MailingAddress", {})
            row = {
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
            }

            if row["orders"] == 0 and row["total_spent"] == 0:
                continue

            all_rows.append(row)
            log_customer(row, len(all_rows))

        log(f"✅ Pulled {len(customers)} customers from skip {skip}")
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

    log(f"📊 Finished fetching. Total rows to sync: {len(all_rows)}")
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
    log("🔥 ENTERED MAIN FUNCTION")
    log("🚀 Starting real-time RICS → Optimizely sync")
    log(f"GITHUB_REF: {os.getenv('GITHUB_REF')}")
    log(f"IS_TEST_BRANCH: {IS_TEST_BRANCH}")
    log(f"RICS_API_TOKEN present? {'✅' if RICS_API_TOKEN else '❌'}")

    rows = fetch_rics_data()
    log(f"📊 Fetched {len(rows)} customers from RICS")

    push_to_optimizely(rows)

    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_export_{timestamp}_{len(rows)}rows.csv"
    csv_path = save_csv(rows, filename)

    upload_to_drive(csv_path)
    log("✅ All done!")

if __name__ == "__main__":
    main()
