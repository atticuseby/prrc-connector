#!/usr/bin/env python3
"""
Test GetCustomerPurchaseHistory endpoint to see if it has more recent data than GetPOSTransaction.
"""
import os
import sys
from datetime import datetime, timedelta
import requests
import json

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

def parse_dt(dt_str):
    """Parse RICS date string."""
    if not dt_str:
        return None
    
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str.split('.')[0], fmt.replace('.%f', ''))
        except:
            continue
    
    return None

def test_get_postransaction(days_back=45, store_code=1):
    """Test GetPOSTransaction endpoint."""
    start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    
    payload = {
        "Take": 100,
        "Skip": 0,
        "TicketDateStart": start_date,
        "TicketDateEnd": end_date,
        "BatchStartDate": start_date,
        "BatchEndDate": end_date,
        "StoreCode": str(store_code)
    }
    
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        return None
    
    try:
        resp = requests.post(
            "https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction",
            headers={"Token": token},
            json=payload,
            timeout=30
        )
        
        if resp.status_code != 200:
            return {"error": f"Status {resp.status_code}"}
        
        data = resp.json()
        sales = data.get("Sales", [])
        
        dates = []
        for sale in sales:
            sale_headers = sale.get("SaleHeaders", [])
            for header in sale_headers:
                dt_str = header.get("TicketDateTime") or header.get("SaleDateTime")
                if dt_str:
                    dt = parse_dt(dt_str)
                    if dt:
                        dates.append(dt)
        
        if dates:
            return {
                "endpoint": "GetPOSTransaction",
                "sales_count": len(sales),
                "oldest_date": min(dates),
                "newest_date": max(dates),
                "days_old": (datetime.utcnow() - max(dates)).days
            }
        else:
            return {"endpoint": "GetPOSTransaction", "sales_count": len(sales), "dates": "none"}
            
    except Exception as e:
        return {"error": str(e)}

def test_get_customer_purchase_history(days_back=45, store_code=1):
    """Test GetCustomerPurchaseHistory endpoint by getting a few customers first."""
    token = os.getenv("RICS_API_TOKEN")
    if not token:
        return None
    
    # First, get some customers from the store
    try:
        customer_resp = requests.post(
            "https://enterprise.ricssoftware.com/api/Customer/GetCustomer",
            headers={"Token": token},
            json={"StoreCode": store_code, "Skip": 0, "Take": 10},
            timeout=30
        )
        
        if customer_resp.status_code != 200:
            return {"error": f"GetCustomer failed: {customer_resp.status_code}"}
        
        customer_data = customer_resp.json()
        customers = customer_data.get("Customers", [])
        
        if not customers:
            return {"error": "No customers found"}
        
        print(f"   Found {len(customers)} customers, testing purchase history for first 5...")
        
        # Test purchase history for first 5 customers
        all_dates = []
        customers_tested = 0
        
        start_date = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        
        for customer in customers[:5]:
            customer_id = customer.get("CustomerId")
            if not customer_id:
                continue
            
            try:
                # Try with date range
                ph_payload = {
                    "CustomerId": customer_id,
                    "Take": 100,
                    "Skip": 0,
                    "StartDate": start_date,
                    "EndDate": end_date
                }
                
                ph_resp = requests.post(
                    "https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory",
                    headers={"Token": token},
                    json=ph_payload,
                    timeout=30
                )
                
                if ph_resp.status_code == 200:
                    ph_data = ph_resp.json()
                    sale_headers = ph_data.get("SaleHeaders", [])
                    
                    for header in sale_headers:
                        dt_str = header.get("TicketDateTime") or header.get("SaleDateTime")
                        if dt_str:
                            dt = parse_dt(dt_str)
                            if dt:
                                all_dates.append(dt)
                    
                    customers_tested += 1
                    
            except Exception as e:
                print(f"   Error testing customer {customer_id}: {e}")
                continue
        
        if all_dates:
            return {
                "endpoint": "GetCustomerPurchaseHistory",
                "customers_tested": customers_tested,
                "oldest_date": min(all_dates),
                "newest_date": max(all_dates),
                "days_old": (datetime.utcnow() - max(all_dates)).days,
                "total_purchases": len(all_dates)
            }
        else:
            return {
                "endpoint": "GetCustomerPurchaseHistory",
                "customers_tested": customers_tested,
                "dates": "none"
            }
            
    except Exception as e:
        return {"error": str(e)}

def main():
    print("=" * 80)
    print("Comparing GetPOSTransaction vs GetCustomerPurchaseHistory")
    print("=" * 80)
    print(f"Current UTC time: {datetime.utcnow()}")
    print()
    
    print("Testing GetPOSTransaction endpoint...")
    pos_result = test_get_postransaction(days_back=45, store_code=1)
    
    print("Testing GetCustomerPurchaseHistory endpoint...")
    cph_result = test_get_customer_purchase_history(days_back=45, store_code=1)
    
    print()
    print("=" * 80)
    print("RESULTS")
    print("=" * 80)
    
    if pos_result and "error" not in pos_result:
        print(f"\n📊 GetPOSTransaction:")
        if "newest_date" in pos_result:
            print(f"   Newest date: {pos_result['newest_date']}")
            print(f"   Days old: {pos_result['days_old']}")
            print(f"   Sales found: {pos_result['sales_count']}")
        else:
            print(f"   {pos_result}")
    else:
        print(f"\n❌ GetPOSTransaction: {pos_result.get('error', 'Unknown error')}")
    
    if cph_result and "error" not in cph_result:
        print(f"\n📊 GetCustomerPurchaseHistory:")
        if "newest_date" in cph_result:
            print(f"   Newest date: {cph_result['newest_date']}")
            print(f"   Days old: {cph_result['days_old']}")
            print(f"   Customers tested: {cph_result['customers_tested']}")
            print(f"   Total purchases: {cph_result['total_purchases']}")
        else:
            print(f"   {cph_result}")
    else:
        print(f"\n❌ GetCustomerPurchaseHistory: {cph_result.get('error', 'Unknown error')}")
    
    print()
    print("=" * 80)
    print("COMPARISON")
    print("=" * 80)
    
    if (pos_result and "newest_date" in pos_result and 
        cph_result and "newest_date" in cph_result):
        
        pos_newest = pos_result["newest_date"]
        cph_newest = cph_result["newest_date"]
        
        if cph_newest > pos_newest:
            days_newer = (cph_newest - pos_newest).days
            print(f"✅ GetCustomerPurchaseHistory has data {days_newer} days newer!")
            print(f"   This endpoint may be better for recent data.")
        elif pos_newest > cph_newest:
            days_newer = (pos_newest - cph_newest).days
            print(f"⚠️  GetPOSTransaction has data {days_newer} days newer.")
        else:
            print(f"📊 Both endpoints return the same newest date: {pos_newest}")
    else:
        print("⚠️  Could not compare - one or both endpoints failed or returned no dates")

if __name__ == "__main__":
    sys.exit(main() if main() else 0)

