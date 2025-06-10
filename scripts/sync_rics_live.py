import os
import csv
import requests
import time
import traceback
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
LOG_DIR = "logs"
SKIP_TRACKER = os.path.join(OUTPUT_DIR, "last_successful_skip.txt")
BATCH_SIZE = 500
STORE_CODE = 12132
MAX_SKIP_LIMIT = 10000
SLOW_RESPONSE_THRESHOLD = 60
ABSOLUTE_TIMEOUT_SECONDS = 120
MAX_RETRIES = 3
IS_TEST_BRANCH = os.getenv("GITHUB_REF", "").endswith("/test")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

log_file_path = os.path.join(LOG_DIR, f"sync_log_{datetime.now().strftime('%m_%d_%Y_%H%M')}.log")
def log(msg):
    timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{timestamp} {msg}"
    print(line, flush=True)
    with open(log_file_path, "a") as f:
        f.write(line + "\n")

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(
        CREDS_PATH, scopes=["https://www.googleapis.com/auth/drive.file"]
    )
    return build("drive", "v3", credentials=creds)

def upload_to_drive(filepath, folder_id):
    service = get_drive_service()
    file_metadata = {"name": os.path.basename(filepath), "parents": [folder_id]}
    media = MediaFileUpload(filepath)
    uploaded = service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    log(f"✅ Uploaded {os.path.basename(filepath)} to Drive as ID: {uploaded['id']}")

def log_customer(c, index):
    log(f"👤 Customer {index}: {c['first_name']} {c['last_name']} | "
        f"Email: {c['email'] or '—'} | Phone: {c['phone'] or '—'} | "
        f"Orders: {c['orders']} | Spent: ${c['total_spent']} | "
        f"{c['city']}, {c['state']} {c['zip']}")

def fetch_rics_data():
    all_rows = []

    try:
        with open(SKIP_TRACKER, "r") as f:
            skip = int(f.read().strip())
    except Exception:
        skip = 0

    while skip < MAX_SKIP_LIMIT:
        log(f"📦 Fetching customers from skip {skip}")

        customers = []
        for attempt in range(MAX_RETRIES):
            try:
                start_time = time.time()
                res = requests.post(
                    url="https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                    headers={"Token": RICS_API_TOKEN},
                    json={"StoreCode": STORE_CODE, "Skip": skip, "Take": 100},
                    timeout=ABSOLUTE_TIMEOUT_SECONDS
                )
                response_time = time.time() - start_time
                log(f"⏱️ Attempt {attempt+1} RICS response time: {response_time:.2f}s")

                if response_time > SLOW_RESPONSE_THRESHOLD:
                    log(f"⚠️ Warning: Response time exceeded {SLOW_RESPONSE_THRESHOLD}s, but continuing.")

                res.raise_for_status()
                customers = res.json().get("Customers", [])
                log(f"✅ RICS response status: {res.status_code} | Retrieved: {len(customers)}")
                break

            except requests.exceptions.HTTPError as e:
                if res.status_code >= 500:
                    log(f"❌ Server error on skip {skip}: {res.status_code} — retrying...")
                else:
                    log(f"❌ Client error on skip {skip}: {res.status_code} — skipping ahead")
                    break
            except Exception as e:
                log(f"❌ Error on attempt {attempt+1}: {e}")
                log(traceback.format_exc())
            time.sleep([0, 10, 30][attempt])

        if not customers:
            if attempt == MAX_RETRIES - 1:
                log(f"❌ Failed after {MAX_RETRIES} attempts on skip {skip}. Aborting sync.")
                raise SystemExit(1)
            log(f"📭 No customers returned at skip {skip} — ending")
            break

        if skip == 0 and customers:
            log(f"📄 RAW sample record:\n{customers[0]}")
            log(f"🧪 OrderCount = {customers[0].get('OrderCount')}, TotalSpent = {customers[0].get('TotalSpent')}")

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
            all_rows.append(row)
            log_customer(row, len(all_rows))

        log(f"✅ Pulled {len(customers)} customers from skip {skip}")
        skip += 100

        with open(SKIP_TRACKER, "w") as f:
            f.write(str(skip))

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
    paths = [os.path.join(OUTPUT_DIR, filename), os.path.join(DATA_DIR, filename)]
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

    if rows:
        push_to_optimizely(rows)
        timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
        filename = f"rics_export_{timestamp}_{len(rows)}rows.csv"
        csv_path = save_csv(rows, filename)
        upload_to_drive(csv_path, GDRIVE_FOLDER_ID_RICS)

    upload_to_drive(log_file_path, GDRIVE_FOLDER_ID_RICS)
    log("✅ All done!")

if __name__ == "__main__":
    main()
