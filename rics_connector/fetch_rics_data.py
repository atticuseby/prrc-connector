import os
import csv
import time
import requests
from datetime import datetime
from scripts.config import RICS_API_TOKEN, TEST_EMAIL
from scripts.helpers import log_message

MAX_SKIP = 100  # Can increase for full production
MAX_RETRIES = 3
MAX_RESPONSE_TIME_SECONDS = 60  # Only warn if exceeded — don’t abort
ABSOLUTE_TIMEOUT_SECONDS = 120  # This ensures the request doesn’t hang forever

# Fields for the detailed purchase history CSV
purchase_history_fields = [
    # Customer info
    "rics_id", "email", "first_name", "last_name", "orders", "total_spent", "city", "state", "zip", "phone",
    # Sale header info
    "TicketDateTime", "TicketNumber", "Change", "TicketVoided", "ReceiptPrinted", "TicketSuspended", "ReceiptEmailed", "SaleDateTime", "TicketModifiedOn", "ModifiedBy", "CreatedOn",
    # Line item info
    "TicketLineNumber", "Quantity", "AmountPaid", "Sku", "Summary", "Description", "SupplierCode", "SupplierName", "Color", "Column", "Row", "OnHand"
]

def fetch_rics_data_with_purchase_history():
    timestamp = datetime.now().strftime("%m_%d_%Y_%H%M")
    filename = f"rics_customer_purchase_history_{timestamp}.csv"
    output_path = os.path.join("data", filename)

    os.makedirs("data", exist_ok=True)

    all_rows = []
    skip = 0

    while skip < MAX_SKIP:
        log_message(f"\n📄 Requesting customers from skip: {skip}...")

        attempt = 0
        customers = []

        while attempt < MAX_RETRIES:
            try:
                start = time.time()
                response = requests.post(
                    url="https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
                    headers={"Authorization": f"Bearer {RICS_API_TOKEN}"},
                    json={"StoreCode": 12132, "Skip": skip, "Take": 100},
                    timeout=ABSOLUTE_TIMEOUT_SECONDS
                )
                duration = time.time() - start
                log_message(f"⏱️ Attempt {attempt+1} response time: {duration:.2f}s")

                response.raise_for_status()
                customers = response.json().get("Customers", [])

                if duration > MAX_RESPONSE_TIME_SECONDS:
                    log_message(f"⚠️ Warning: Response exceeded {MAX_RESPONSE_TIME_SECONDS}s, but continuing anyway.")

                if customers:
                    break  # Success
                else:
                    log_message(f"⚠️ No customers returned on attempt {attempt+1}. Retrying...")
                    attempt += 1
                    time.sleep(3)

            except Exception as e:
                log_message(f"❌ Attempt {attempt+1} failed: {e}")
                attempt += 1
                time.sleep(3)

        if attempt == MAX_RETRIES:
            log_message("❌ Aborting: RICS failed or unresponsive after 3 attempts.")
            raise SystemExit(1)

        for customer in customers:
            mailing = customer.get("MailingAddress", {})
            customer_info = {
                "rics_id": customer.get("CustomerId"),
                "email": customer.get("Email", "").strip(),
                "first_name": customer.get("FirstName", "").strip(),
                "last_name": customer.get("LastName", "").strip(),
                "orders": customer.get("OrderCount", 0),
                "total_spent": customer.get("TotalSpent", 0),
                "city": mailing.get("City", "").strip(),
                "state": mailing.get("State", "").strip(),
                "zip": mailing.get("PostalCode", "").strip(),
                "phone": customer.get("PhoneNumber", "").strip()
            }
            # Fetch purchase history for this customer
            cust_id = customer.get("CustomerId")
            if not cust_id:
                continue
            ph_skip = 0
            ph_take = 100
            while True:
                try:
                    ph_response = requests.post(
                        url="https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory",
                        headers={"Authorization": f"Bearer {RICS_API_TOKEN}"},
                        json={"CustomerId": cust_id, "Take": ph_take, "Skip": ph_skip},
                        timeout=ABSOLUTE_TIMEOUT_SECONDS
                    )
                    ph_response.raise_for_status()
                    ph_data = ph_response.json()
                    if not ph_data.get("IsSuccessful"):
                        log_message(f"⚠️ Purchase history failed for customer {cust_id}: {ph_data.get('Message')}")
                        break
                    sale_headers = ph_data.get("SaleHeaders", [])
                    if not sale_headers:
                        break
                    for sale in sale_headers:
                        sale_info = {
                            "TicketDateTime": sale.get("TicketDateTime"),
                            "TicketNumber": sale.get("TicketNumber"),
                            "Change": sale.get("Change"),
                            "TicketVoided": sale.get("TicketVoided"),
                            "ReceiptPrinted": sale.get("ReceiptPrinted"),
                            "TicketSuspended": sale.get("TicketSuspended"),
                            "ReceiptEmailed": sale.get("ReceiptEmailed"),
                            "SaleDateTime": sale.get("SaleDateTime"),
                            "TicketModifiedOn": sale.get("TicketModifiedOn"),
                            "ModifiedBy": sale.get("ModifiedBy"),
                            "CreatedOn": sale.get("CreatedOn")
                        }
                        for item in sale.get("CustomerPurchases", []):
                            item_info = {
                                "TicketLineNumber": item.get("TicketLineNumber"),
                                "Quantity": item.get("Quantity"),
                                "AmountPaid": item.get("AmountPaid"),
                                "Sku": item.get("Sku"),
                                "Summary": item.get("Summary"),
                                "Description": item.get("Description"),
                                "SupplierCode": item.get("SupplierCode"),
                                "SupplierName": item.get("SupplierName"),
                                "Color": item.get("Color"),
                                "Column": item.get("Column"),
                                "Row": item.get("Row"),
                                "OnHand": item.get("OnHand")
                            }
                            row = {**customer_info, **sale_info, **item_info}
                            all_rows.append(row)
                    # Pagination
                    result_stats = ph_data.get("ResultStatistics", {})
                    end_record = result_stats.get("EndRecord", 0)
                    total_records = result_stats.get("TotalRecords", 0)
                    if end_record >= total_records:
                        break
                    ph_skip += ph_take
                except Exception as e:
                    log_message(f"❌ Error fetching purchase history for customer {cust_id}: {e}")
                    break
        skip += 100

    # Write to CSV
    with open(output_path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=purchase_history_fields)
        writer.writeheader()
        writer.writerows(all_rows)
    log_message(f"📝 Wrote purchase history CSV to: {output_path}")

    return output_path

if __name__ == "__main__":
    fetch_rics_data_with_purchase_history()
