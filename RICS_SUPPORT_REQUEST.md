# RICS API Support Request

## Issue Summary
We are experiencing 401 Unauthorized errors when trying to access the RICS API. Our integration was working previously but has stopped functioning. We need a valid API token to access POS transaction data.

## Technical Details

### API Endpoint
- **URL**: `https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction`
- **Method**: POST
- **Authentication**: Token-based (header: `Token`)

### Request Format
```json
{
  "Take": 100,
  "Skip": 0,
  "TicketDateStart": "2024-01-01T00:00:00Z",
  "TicketDateEnd": "2024-12-31T23:59:59Z",
  "BatchStartDate": "2024-01-01T00:00:00Z",
  "BatchEndDate": "2024-12-31T23:59:59Z",
  "StoreCode": 1
}
```

### Headers
```
Content-Type: application/json
Token: [API_TOKEN_VALUE]
```

### Store Codes We Need Access To
We need to access data from multiple stores:
- Store Codes: 1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 21, 22, 98, 99

### Data Requirements
- **Primary Data**: POS transaction data (sales, customers, purchase history)
- **Date Range**: Last 7-30 days of transaction data
- **Frequency**: Daily automated data pulls
- **Purpose**: Customer data integration with marketing platforms (Optimizely, Meta)

### Error Details
- **Error Code**: 401 Unauthorized
- **Response Time**: ~0.12-0.15 seconds (API is responding)
- **Consistency**: 100% failure rate across all store codes
- **Previous Status**: Was working before (had valid token previously)

## What We Need
1. **New API Token** with access to POS transaction data
2. **Confirmation** that the endpoint `https://enterprise.ricssoftware.com/api/POS/GetPOSTransaction` is still active
3. **Verification** that our store codes (1,2,3,4,6,7,8,9,10,11,12,21,22,98,99) are accessible
4. **Documentation** on any recent changes to the API authentication or endpoint

## Additional Endpoints We May Need
Based on our codebase, we also use:
- `https://enterprise.ricssoftware.com/api/Customer/GetCustomer`
- `https://enterprise.ricssoftware.com/api/Customer/GetCustomerPurchaseHistory`

## Contact Information
- **Company**: [Your Company Name]
- **Contact**: [Your Name]
- **Email**: [Your Email]
- **Phone**: [Your Phone]

## Urgency
This is blocking our daily customer data synchronization process. We need this resolved ASAP to maintain our marketing automation workflows.

---

**Please provide:**
1. A new API token with the required permissions
2. Confirmation that the endpoints are still active
3. Any documentation about recent API changes
4. Expected response time for this request

Thank you for your assistance!

