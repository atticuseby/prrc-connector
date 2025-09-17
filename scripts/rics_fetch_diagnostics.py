import os
import sys
import csv
import json
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone

import requests

# Optional GDrive uploader (won't crash if missing)
UPLOAD_AVAILABLE = False
try:
    from upload_to_gdrive import upload_to_drive  # scripts/ same folder
    UPLOAD_AVAILABLE = True
except Exception:
    pass

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("rics_diag")

# ---------- Config from ENV / args ----------
DEFAULT_STORE_CODES = [1,2,3,4,6,7,8,9,10,11,12,21,22,98,99]

ENTERPRISE_URL = "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction"
PUBLIC_URL     = "https://api.ricssoftware.com/pos/GetPOSTransaction"

AUTH_STYLES = ("token", "bearer")       # try both
DATE_PARAM_STYLES = ("ticket", "sale")  # try both

CSV_FIELDS = [
    "TicketDateTime","TicketNumber","SaleDateTime","StoreCode","TerminalId","Cashier",
    "AccountNumber","CustomerId","Sku","Description","Quantity","AmountPaid","Discount",
    "Department","SupplierName"
]

# ---------- Helpers ----------
def parse_dt(dt_str):
    if not dt_str:
        return None
    fmts = (
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ",
        "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"
    )
    for fmt in fmts:
        try:
            return datetime.strptime(dt_str, fmt)
        except Exception:
            continue
    return None

def zone_aware_window(days):
    """Return two windows: pure UTC and ET->UTC converted."""
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=days)

    # America/New_York local interpretation, then convert to UTC string for API
    try:
        from zoneinfo import ZoneInfo
        et = ZoneInfo("America/New_York")
        now_et = datetime.now(et)
        start_et = now_et - timedelta(days=days)
        # Convert local ET moments to UTC timestamps
        start_et_as_utc = start_et.astimezone(timezone.utc)
        now_et_as_utc = now_et.astimezone(timezone.utc)
    except Exception:
        # Fallback: just use UTC window twice if zoneinfo missing
        start_et_as_utc = start_utc
        now_et_as_utc = now_utc

    fmt = "%Y-%m-%dT%H:%M:%SZ"
    return (
        (start_utc.strftime(fmt), now_utc.strftime(fmt)),               # ("utc", ...)
        (start_et_as_utc.strftime(fmt), now_et_as_utc.strftime(fmt)),   # ("et_to_utc", ...)
    )

def build_payload(store_code, skip, take, date_style, start, end):
    if date_style == "ticket":
        dr = {"TicketDateStart": start, "TicketDateEnd": end}
    else:
        dr = {"SaleDateStart": start, "SaleDateEnd": end}
    payload = {
        "Take": take,
        "Skip": skip,
        "StoreCode": store_code,
        **dr,
    }
    return payload

def build_headers(auth_style, token):
    if auth_style == "token":
        return {"Token": token}
    return {"Authorization": f"Bearer {token}"}

def extract_sales(json_obj):
    # Normalize various possible keys RICS might use
    if isinstance(json_obj, dict):
        for key in ("Sales", "sales", "Transactions", "transactions"):
            if key in json_obj and isinstance(json_obj[key], list):
                return json_obj[key]
    return []

def sale_to_rows(sale, already_seen):
    rows = []
    sale_dt = parse_dt(sale.get("TicketDateTime") or sale.get("SaleDateTime"))
    # We filter by API window; no need to re-filter here except for sanity
    sale_info = {
        "TicketDateTime": sale.get("TicketDateTime"),
        "TicketNumber": sale.get("TicketNumber"),
        "SaleDateTime": sale.get("SaleDateTime"),
        "StoreCode": sale.get("StoreCode"),
        "TerminalId": sale.get("TerminalId"),
        "Cashier": sale.get("CashierName"),
        "AccountNumber": sale.get("AccountNumber"),
        "CustomerId": sale.get("CustomerId"),
    }
    for item in sale.get("SaleLines", []) or []:
        key = f"{sale_info.get('TicketNumber')}_{item.get('Sku')}"
        if key in already_seen:
            continue
        already_seen.add(key)
        rows.append({
            **sale_info,
            "Sku": item.get("Sku"),
            "Description": item.get("Description"),
            "Quantity": item.get("Quantity"),
            "AmountPaid": item.get("AmountPaid"),
            "Discount": item.get("DiscountAmount"),
            "Department": item.get("Department"),
            "SupplierName": item.get("SupplierName"),
        })
    return rows

def write_csv(path, rows, headers=CSV_FIELDS):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in headers})

# ---------- Main diagnostic fetch ----------
def diagnose_and_fetch(days, pages, per_page, debug, limit_stores):
    token = (os.getenv("RICS_API_TOKEN") or "").strip()
    if not token:
        logger.error("Missing RICS_API_TOKEN")
        sys.exit(1)

    env_codes = (os.getenv("RICS_STORE_CODES") or "").strip()
    if env_codes:
        try:
            store_codes = [int(x.strip()) for x in env_codes.split(",") if x.strip()]
        except Exception:
            logger.warning("Failed to parse RICS_STORE_CODES; using fallback list.")
            store_codes = DEFAULT_STORE_CODES[:]
    else:
        store_codes = DEFAULT_STORE_CODES[:]

    if limit_stores:
        store_codes = store_codes[:limit_stores]

    logger.info(f"Store codes to test: {store_codes}")
    utc_win, et_win = zone_aware_window(days)
    logger.info(f"UTC Window:      {utc_win[0]} → {utc_win[1]}")
    logger.info(f"ET→UTC Window:   {et_win[0]} → {et_win[1]}")

    endpoints = [ENTERPRISE_URL, PUBLIC_URL]
    date_windows = [("utc", utc_win), ("et_to_utc", et_win)]

    results_rows = []
    seen = set()

    # Summary tracker
    tried_matrix = {}  # store_code -> list of tuples with outcome

    for store in store_codes:
        tried_matrix[store] = []
        store_success = False

        for endpoint in endpoints:
            for auth_style in AUTH_STYLES:
                headers = build_headers(auth_style, token)

                for date_style in DATE_PARAM_STYLES:
                    for win_name, (start, end) in date_windows:

                        page = 0
                        skip = 0
                        collected_for_combo = 0
                        error_text = None

                        while page < pages:
                            payload = build_payload(store, skip, per_page, date_style, start, end)
                            try:
                                if debug:
                                    logger.info(f"[TRY] store={store} ep={'enterprise' if endpoint==ENTERPRISE_URL else 'public'} "
                                                f"auth={auth_style} date={date_style}/{win_name} page={page+1} "
                                                f"payload={payload}")

                                resp = requests.post(endpoint, headers=headers, json=payload, timeout=45)
                                status = resp.status_code
                                ctype = resp.headers.get("Content-Type","")
                                body = resp.text

                                if status >= 400:
                                    # Capture a short body excerpt for diagnosis
                                    excerpt = body[:600].replace("\n"," ")
                                    logger.warning(f"[{status}] store={store} ep={endpoint} auth={auth_style} "
                                                   f"date={date_style}/{win_name} page={page+1} "
                                                   f"ctype={ctype} excerpt={excerpt}")
                                    error_text = f"HTTP {status}"
                                    break

                                # Try JSON parse
                                try:
                                    data = resp.json()
                                except Exception as je:
                                    excerpt = body[:600].replace("\n"," ")
                                    logger.warning(f"[JSON?] store={store} parse error: {je}; excerpt={excerpt}")
                                    error_text = "Invalid JSON"
                                    break

                                # Dump raw keys in debug mode
                                if debug:
                                    logger.info(f"Top-level keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")

                                sales = extract_sales(data)
                                if debug:
                                    logger.info(f"Sales len={len(sales)}")

                                if not sales:
                                    # No data on this page; stop paging this combo
                                    break

                                # Convert to rows
                                page_rows = []
                                for sale in sales:
                                    page_rows.extend(sale_to_rows(sale, seen))
                                collected_for_combo += len(page_rows)
                                results_rows.extend(page_rows)

                                # paging
                                if len(sales) < per_page:
                                    break
                                page += 1
                                skip += per_page
                                time.sleep(0.2)

                            except requests.exceptions.RequestException as rexc:
                                logger.error(f"Request error store={store}: {rexc}")
                                error_text = str(rexc)
                                break
                            except Exception as ex:
                                logger.error(f"Unexpected error store={store}: {ex}")
                                error_text = str(ex)
                                break

                        # record outcome for this combo
                        tried_matrix[store].append({
                            "endpoint": ("enterprise" if endpoint==ENTERPRISE_URL else "public"),
                            "auth": auth_style,
                            "date": f"{date_style}/{win_name}",
                            "rows": collected_for_combo,
                            "error": error_text
                        })

                        if collected_for_combo > 0:
                            store_success = True
                            # Use first working combo per store to avoid duplicates
                            break
                    if store_success: break
                if store_success: break
            if store_success: break

        if not store_success and debug:
            logger.warning(f"Store {store}: no rows with any combo.")

    # ---------- Write outputs ----------
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_dir = "."
    base_ts = f"rics_customer_purchase_history_{ts}.csv"
    latest = "rics_customer_purchase_history_latest.csv"
    dedup = "rics_customer_purchase_history_deduped.csv"

    if results_rows:
        write_csv(os.path.join(out_dir, base_ts), results_rows)
        write_csv(os.path.join(out_dir, latest), results_rows)

        # Dedup by TicketNumber+Sku
        dd_seen = set()
        dedup_rows = []
        for r in results_rows:
            k = f"{r.get('TicketNumber')}_{r.get('Sku')}"
            if k in dd_seen:
                continue
            dd_seen.add(k)
            dedup_rows.append(r)
        write_csv(os.path.join(out_dir, dedup), dedup_rows)
        logger.info(f"Final counts → raw: {len(results_rows)}, deduped: {len(dedup_rows)}")
    else:
        # EMPTY marker with reason
        empty = base_ts.replace(".csv", "_EMPTY.csv")
        write_csv(os.path.join(out_dir, empty), [])
        logger.warning("No rows collected. Wrote EMPTY CSV with headers only.")

    # ---------- Print human summary matrix ----------
    logger.info("=== Diagnostic summary per store (first working combo used) ===")
    for store, outcomes in tried_matrix.items():
        for o in outcomes:
            logger.info(f"store={store:>3} ep={o['endpoint']:<10} auth={o['auth']:<6} "
                        f"date={o['date']:<18} rows={o['rows']:<5} error={o['error']}")

    # ---------- Optional: Upload to Drive ----------
    if UPLOAD_AVAILABLE:
        try:
            if results_rows:
                upload_to_drive(base_ts)
                upload_to_drive(latest)
                upload_to_drive(dedup)
            else:
                upload_to_drive(empty)
        except Exception as ue:
            logger.error(f"GDrive upload failed: {ue}")

    # Exit code hint for CI
    if not results_rows:
        # Exit 0 so workflow can proceed, but logs + _EMPTY.csv will show issue
        logger.warning("Run completed with 0 rows. See matrix above to fix params.")
    else:
        logger.info("Run completed with data. 🎉")

def main():
    p = argparse.ArgumentParser(description="RICS Fetch Diagnostics (one-shot).")
    p.add_argument("--days", type=int, default=7, help="Lookback window in days (default: 7)")
    p.add_argument("--pages", type=int, default=3, help="Max pages to try per combo (default: 3)")
    p.add_argument("--per-page", type=int, default=100, help="Take/page size (default: 100)")
    p.add_argument("--debug", action="store_true", help="Verbose debug logging")
    p.add_argument("--limit-stores", type=int, default=0, help="Limit number of stores for faster debug")
    args = p.parse_args()

    if args.debug:
        logger.setLevel(logging.DEBUG)

    diagnose_and_fetch(
        days=args.days,
        pages=args.pages,
        per_page=args.per_page,
        debug=args.debug,
        limit_stores=args.limit_stores if args.limit_stores > 0 else None
    )

if __name__ == "__main__":
    main()
