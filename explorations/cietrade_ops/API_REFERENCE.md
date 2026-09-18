# cieTrade.net REST API Reference

Source: https://cietrade.helpscoutdocs.com (29 articles fetched 2026-09-18; all read successfully, none unread). Field meanings are inferred from field names and sample values where the docs give none.

## Overview

1. **Base URL:** `https://api.cietrade.net/` ("API Root Endpoint"). Every resource is a URL directly under the root, e.g. `https://api.cietrade.net/ListAccounts`.
2. **Auth:** two things on every call: (a) `Authorization: Bearer <api token>` header (token from Settings > Integration > Api Token; user must have "Can Access cieTrade API" checked in Settings > Users & Roles; role needs "Allow Generation of API Token" to regenerate, ADMIN role only by default); (b) `UserID=<cieTrade login email>` as a query-string parameter on every request. `UserID` is Required on every endpoint. (One legacy example shows `UserPwd=`; ignore, not documented.)
3. **Methods:** the API is documented as GET-only for reads ("current API version only provides support for GET methods"). Write endpoints exist: `CreateDispatch` (POST), `UpdateDispatch` (PUT), `CreateServiceRecord` (POST, JSON body), `UpdateServiceRecord` (PUT, JSON body). Write endpoints keep `UserID` on the query string.
4. **Response:** JSON array of flat objects (one object per row). Dates in examples appear as `yyyy-mm-dd`, `yyyy-mm-ddT00:00:00`, `mm-dd-yy`, or `Oct 16 2025 12:51PM` depending on endpoint; request dates are written `m/d/yyyy` or `mm/dd/yyyy`.
5. **Errors:** returned as a plain text string / single field rather than an HTTP status code, e.g. `"Source is a required field"`, `"[DispatchID] is a required field"`, `ERRORS: [Field]: message`. Customer Ledger doc: "Errors are returned as a single ERROR field describing the problem, rather than as an HTTP status code."
6. **Pagination:** none. No page/offset parameters exist anywhere; use date windows. Row caps: SystemLog 10,000; ListServices / GetServiceRecordFull 75,000; ListServiceExpenses 50,000. Default date window on most endpoints is 30 days before today to today.
7. **Rate limits:** currently none ("may change going forward").
8. **Receiving / inbound (purchase receipts, "PR" worksheets):** `ListWorksheets` with `InventoryType=RCV` (header, one row per load), `ListWorksheetDetails` (grade lines), `ListWorksheetExpenses` (freight/expense lines), `TradingInquiry` with `InventoryType=RCV` (grade lines with margin). Inventory receipts have Customer `(INV)`.
9. **Purchase orders / sales orders:** `ListOrders` (`Source=PO` or `Source=SO`) for headers, `ListOrderDetails` for product lines. **Shipments / sales:** `ListWorksheets` with `InventoryType=SHIP` (supplier shows `(INV)`), `ListWorksheetDetails`, `TradingInquiry` with `InventoryType=SHIP`. Worksheet rows carry `PoNo`/`SoNo` to link to orders.
10. **Inventory:** `ListInventory` (serial/finished-goods items only, bulk excluded), `ListConvertingJobs` (warehouse processing jobs). **Dispatch / service:** `ListDispatchJobs`, `ListServices`, `ListServiceExpenses`, `GetServiceRecordFull`, `ListBillingSheets`, `ListBillingSheetCharges`. **Master data:** `ListAccounts`, `ListAccountLocations`, `ListContacts`. **Finance:** `ListAccountsReceivable`, `ListCustomerLedger`, `ListAdjustments`, `VoucherInquiry`, `PostedPayables`. **Audit:** `SystemLog`.

Common parameter conventions (from the docs): account-type filters accept "exact company name as defined" or the numeric Account ID; `Dept` accepts the department code "as it is stored in cieTrade" (e.g. `00`) or the Department Short Name; `AsOf` (where present) returns records last updated on/after that date and causes all other filters to be ignored.

---

## GET /ListAccounts  (Accounts / Counterparties)

Article: /article/1431-accounts-counterparties. One row per counterparty (customer, supplier, or expense vendor). Ordered by last updated, most recent first.
Example: `https://api.cietrade.net/ListAccounts?UserID=ExampleUser@gmail.com&Account=daktest234&Role=Customer`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | cieTrade.net user name |
| `Account` | Optional | exact company name, Account ID, or blank for all. If used only one row is returned |
| `Role` | Optional | `"Customer"` or `"Vendor"`; blank returns all types |
| `AsOf` | Optional | records last updated on/after this date; other filters ignored when provided |

Response fields: `account_id` (counterparty ID), `account_name`, `role_name` (e.g. `"Customer,Vendor"`), `payment_terms`, `active_status` (`Active`), `invoice_note`, `currency_code`, `reference_id`, `supplier_GLAcct`, `expense_GLAcct`, `credit_limit`, `primary_contact`, `primary_email` (may be `;`-separated), `billing_contact`, `billing_email`, `business_Phone`, `address_line_1`, `address_line_2`, `address_line_3`, `city`, `region`, `postal`, `country`, `LastUpdated`.

## GET /ListAccountLocations  (Account Locations)

Article: /article/1426-locations. One row per location (address) of an account. Ordered by last updated, most recent first.
Example: `https://api.cietrade.net/ListAccountLocations?UserID=ExampleUser@gmail.com&Account=ABCFibers`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `Account` | Optional | exact company name, Account ID, or blank for locations across all accounts |
| `Location` | Optional | Location Short Name; returns only that one record |
| `AsOf` | Optional | last-updated on/after date; other filters ignored |

Response fields: `account_name`, `location_short_name`, `address_id`, `account_id`, `company_primary_email`, `address_line_1`, `address_line_2`, `city`, `region`, `postal`, `country`, `is_primary_address` (`Y`/`N`, only one per counterparty), `contact_name`, `contact_email`, `billing_email_address` (counterparty default billing contact, else primary contact), `active_status`, `location_UDF1`..`location_UDF4`, `address_customs_code`, `LastUpdated`.

## GET /ListContacts  (Account Contacts)

Article: /article/1504-contacts. One row per contact person.
Example (as printed): `https://api.cietrade.net/ListContacts?userID=Example@cietrare.com&name=&Status=&Role=&Location`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `Account` | Optional | exact company name or Account ID |
| `Location` | Optional | Location Short Name; returns only contacts from that location |
| `Status` | Optional | `Active`, `Inactive`, `Prospect`; empty returns any |
| `Role` | Optional | contact role as defined in cieTrade; blank = all |

Response fields: `contact_name`, `account_id`, `account_name`, `location`, `email`, `business_phone`, `mobile_phone`, `active` (`True`/`False`), `notes`, `first_name`, `last_name`, `primary_contact` (`True`/`False`), `job_title`, `active_status`, `role_name`.

## GET /ListAccountsReceivable  (Accounts Receivable)

Article: /article/1427-accounts-receivable. One row per open invoice (A/R report, current or as-of). No invoice detail. Empty if A/R module disabled.
Example: `https://api.cietrade.net/ListAccountsReceivable?UserID=ExampleUser@gmail.com&AsOfDate=06/20/2024&Dept=05&Customer=84698&PastDueOnly=true`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `AsOfDate` | Optional | reporting date; defaults to today |
| `Dept` | Optional | dept code (e.g. `00`) or Department Short Name |
| `Customer` | Optional | exact customer name or account ID; blank = all |
| `PastDueOnly` | Optional | `"true"` or `"false"`; default `false` |

Response fields: `invoice_id`, `group_invoice_id`, `customer`, `customer_id`, `department`, `posting_date`, `payment_date` (`OPEN` when unpaid), `due_date`, `amount` (original), `payments` (applied), `payment_terms`, `balance`, `hc_balance` (home currency), `last_payment_date`, `past_due` (days), `location_name`, `created_by`, `billingUDF1`.

## GET /ListCustomerLedger  (Customer Ledger)

Article: /article/1680-customer-ledger. A/R activity (invoices, payments, memos, adjustments, interest) for one or all customers; same logic as the Statement report. Sorted by customer, then currency, then invoice number.
Example: `https://api.cietrade.net/ListCustomerLedger?UserID=ExampleUser@gmail.com&DateFrom=3/15/2025&DateTo=4/15/2025&CpID=84700` (omit or empty `CpID=` for all accounts)

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateFrom` | Optional | invoice date range FROM; defaults to one month before today |
| `DateTo` | Optional | invoice date range TO; defaults to today |
| `CpID` | Optional | Counterparty-ID of customer; blank = all |

Date window filters on invoice date; payments/memos/adjustments are included whenever they apply to an invoice in range, even if posted later.
Response fields: `CpID`, `Customer`, `InvoiceNo` (invoice the row belongs to), `Transaction` (`INVOICE`, payment posting type such as `CRE` or `UAC`, `CREDIT MEMO`, `DEBIT MEMO`, or adjustment/interest reason e.g. `SALES DISCOUNT`), `DocumentNo` (invoice no, `Pmt# <check no>`, or `<reason>/<invoice>`), `Date`, `DueDate` (null on non-invoice rows), `PastDue` (days as of today; 0 when settled), `CurrencyCd`, `Amount` (transaction currency; payments/credits negative), `Balance` (open balance on invoice), `StatementNo` (grouped invoice number, else null).

## GET /ListAdjustments  (Adjustments)

Article: /article/1485-adjustments. Sales OR purchase adjustments against worksheets in "posted" status for a period. Staged/pending adjustments excluded.
Example: `https://api.cietrade.net/ListAdjustments?UserID=ExampleUser@gmail.com&DateFrom=3/15/2025&DateTo=4/15/2025&Type=S`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateFrom` | Required | adjustment posting date range FROM |
| `DateTo` | Required | adjustment posting date range TO |
| `Account` | Optional | name or Counterparty-ID of customer or vendor |
| `Type` | Optional | `"S"` = Sales Adjustments, `"P"` = Purchase Adjustments; defaults to Sales if blank |

Response fields: `Date`, `Memo Type` (e.g. `Credit Memo`), `Account`, `Reason`, `Wks No` (worksheet), `Memo No`, `Explanation`, `Amount`, `Currency`, `FxAmount`.

## GET /ListBillingSheets  (Billing Sheets)

Article: /article/1428-billing-sheets. Billing Sheet header records (waste brokerage monthly billing per customer/location/hauler). Details via ListBillingSheetCharges.
Example: `https://api.cietrade.net/ListBillingSheets?UserID=ExampleUser@gmail.com&DateFrom=5/21/2024&DateTo=6/12/2024&SearchType=Invoice&Account=TargetAccount&Location=TargetLocation&Dept=DeptID&Status=Status&Vendor=VendorName` (note: example uses `SearchType`, table documents `DateType`)

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `Account` | Optional | exact company name, Account ID, or blank for all |
| `Location` | Optional | location, or blank for all; cannot be used without `Account` |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Optional | default today |
| `DateType` | Optional | `"Invoice"` or `"Transaction"`; default Invoice Date |
| `Dept` | Optional | dept code or short name |
| `Vendor` | Optional | hauler/vendor exact name or account ID; blank = all |
| `Status` | Optional | `Open`, `Completed`, `Approved`, `Posted`, `Not Approved`, `Cancelled`; blank = all |

Response fields: `invoice_no`, `group_invoice_no`, `account_name`, `location_name`, `department`, `hauler`, `status`, `billing_reference`, `invoice_date`, `posting_date`, `due_date`, `sales`, `cost`, `profit`, `weight`, `total_qtyuom`, `owner`, `transaction_date`, `billing_address1`, `billing_address2`, `billing_city`, `billing_region`, `billing_postal_code`, `BillingUDF1`, `account_id`, `notes`.

## GET /ListBillingSheetCharges  (Billing Sheet Charges)

Article: /article/1425-billing-charges. One row per billing-sheet charge line with its matching vendor expense and header context.
Example: `https://api.cietrade.net/ListBillingSheetCharges?UserID=Example@gmail.com&DateFrom=6/1/2024&DateTo=7/2/2024&DateType=Invoice&Account=daktest234&Location=new location&Dept=AMI Trading&Status=OPEN&Vendor=84699&Equipment=""&ServiceType=""&InvoiceNo=1945675`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `InvoiceNo` | (not marked) | specific `Invoice_no` or `group_invoice_no`; overrides all other filters |
| `Account` | Optional | exact name, Account ID, or blank |
| `Location` | Optional | account-location or blank |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Optional | default today |
| `DateType` | Optional | `Invoice` or `Transaction`; default Invoice Date |
| `Dept` | Optional | dept code or short name |
| `Status` | Optional | `Open`, `Completed`, `Approved`, `Posted`, `Not Approved`, `Cancelled` |
| `Vendor` | Optional | exact company name or vendor account ID |
| `Equipment` | Optional | exact equipment type name |
| `ServiceType` | Optional | `Manual`, `On Call`, `Recurring`; blank = all |

Response fields: `invoice_no`, `group_invoice_no`, `account_name`, `location_name`, `billing_reference`, `department`, `owner`, `status`, `transaction_date`, `hauler_name`, `hauler_ID`, `service_GLAcct`, `service_description`, `equipment_name`, `site_ref`, `eq_number`, `weight`, `WeightUOM`, `pono`, `jobno`, `charge_amount`, `cost_per`, `price`, `price_per`, `cost_amount`, `sales_amount`, `margin`, `billing_address1`, `billing_address2`, `billing_city`, `billing_region`, `billing_postal_code`, `BillingUDF1`, `ServiceID` (unique ID of the connected Service Record), `grade`, `address_id`, `account_id`.

## GET /ListDispatchJobs  (Dispatch Jobs)

Article: /article/1430-dispatch-jobs. One row per on-demand dispatch job (customer, location, provider, scheduled date/time, equipment).
Example: `https://api.cietrade.net/ListDispatchJobs?UserID=ExampleUser@gmail.com&DateFrom=05/05/2024&DateTo=06/05/2024&Type=Schedule&AccountName=TargetAccount&Location=TargetLocation` (note: example uses `Type` and `AccountName`; table documents `DateType` and `Account`)

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `Account` | Optional | exact company name, Account ID, or blank for all |
| `Location` | Optional | dispatch job location; cannot be used without `Account` |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Optional | default today |
| `DateType` | Optional | `"Schedule"` or `"Dispatch"` (scheduled date vs job create date); default Schedule |
| `Dept` | Optional | dept code or short name |
| `Vendor` | Optional | hauler/vendor exact name or account ID; blank = all |

Response fields: `job_id`, `account_name`, `location_name`, `schedule_date`, `schedule_time`, `equipment_type`, `job_type`, `vendor_name`, `status` (e.g. `OPEN`), `material`, `completed_date`, `completed_time`, `department`, `dispatch_date` (create date), `dispatch_time`, `requested_by`, `invoice_id`, `username`, `weight`, `weight_unit_of_measure`, `service_address`, `notes`, `dispatch_source`, `dispatch_notes`, `questions_to_ask`, `service_id` (linked service record).

## POST /CreateDispatch  (Dispatch Job Create) - write, summary only

Article: /article/1446-dispatch-job-save. Creates a dispatch job; all values passed as query parameters (`UserID`, `Account`, `Location`, `TypeOfService`, `DispatchDate`, `ScheduleDate`, `User`, `Status` required; `Status` values `OPEN`,`DISPATCHED`,`SERVICED`,`COMPLETED`,`CANCELLED`,`FAILED`).
Returns the new dispatch job ID as a string (e.g. `"372820"`) or a text string listing missing/invalid fields.

## PUT /UpdateDispatch  (Dispatch Job Update) - write, summary only

Article: /article/1590-dispatch-job-update. Updates select fields on an existing job; `UserID` and `DispatchID` required, everything else optional and unchanged if omitted (`WeightUOM` values `LBS`,`KG`,`MT`,`ST`,`EA`; `Priority` `Low`,`Medium`,`High`).
Returns the dispatch job ID as a string on success; `"[DispatchID] is a required field"` if missing.

## GET /ListInventory  (Inventory List)

Article: /article/1505-inventory-list. Serial / Finished Goods inventory items only; bulk inventory is NOT included.
Example: `https://api.cietrade.net/ListInventory?UserID=example@cietrade.com&DateFrom=6/1/2025&DateTo=6/20/2025&DateType=Rcv&Warehouse=""&WarehouseLoc=""&InvClass=""&Supplier=""&LotNo=""`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateFrom` | Required | date range FROM |
| `DateTo` | Required | date range TO |
| `DateType` | Optional | `"POST"` (posted date) or `"RCV"` (received date) |
| `Supplier` | Optional | supplier name or counterparty ID |
| `Warehouse` | Optional | warehouse name; default all |
| `WarehouseLoc` | Optional | location within warehouse |
| `InvClass` | Optional | inventory class name as defined in common information |
| `LotNo` | Optional | item number (lot#); if provided all other filters ignored, one item returned |

Response fields: `item_no` (lot#), `grade_name`, `serial_number`, `quantity_on_hand`, `original_quantity`, `original_tare_quantity`, `original_gross_quantity`, `weight_uom`, `hc_value` (home-currency value), `home_currency`, `value`, `currency`, `condition`, `received_date`, `posed_date` (sic, posted date), `worksheet_no`, `order_number`, `supplier`, `warehouse_name`, `warehouse_location`, `comments`, `inventory_class`, `product_group`, `product_category`, `equipment_number`, `buy_rep`, `age_days`, `unit_cost` (e.g. `123/LBS`), `actual_cost`, `reserved` (`True`/`False`), `property_set` (e.g. `Roll`), `unit_count`, `color`, `specifications`, `roll_basis_weight`, `roll_basis_weight_uom`, `roll_caliper`, `roll_width`, `roll_width_uom`, `roll_diameter`, `roll_diameter_uom`, `roll_core_size`, `roll_core_uom`, `roll_linear_length`, `roll_linear_length_uom`, `plastic_type`, `plastic_form`, `plastic_melt`, `UDF1`..`UDF5`.

## GET /ListConvertingJobs  (Converting Jobs)

Article: /article/1689-list-converting-jobs. Warehouse processing jobs (input inventory -> output products on a machine). Same filters/stored procedure as the Converting Job Inquiry screen. Ordered by Job Date, oldest first.
Example: `https://api.cietrade.net/ListConvertingJobs?UserID=ExampleUser@gmail.com&DateFrom=01/01/2026&DateTo=09/11/2026&DateType=JOB&WarehouseStatus=IN PROCESS`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `JobNo` | Optional | specific job; all other filters ignored |
| `DateType` | Optional | `"JOB"` or `"POST"`; default Job Date |
| `DateFrom` | Optional | both blank = no date filter (all dates); only `DateTo` given -> `DateFrom` = 30 days before today |
| `DateTo` | Optional | only `DateFrom` given -> `DateTo` = today |
| `Warehouse` | Optional | exact warehouse name; blank = all |
| `WarehouseStatus` | Optional | `"SCHEDULED"`, `"IN PROCESS"`, `"COMPLETED"`; blank = all |
| `Status` | Optional | `"WORK"` (not posted) or `"POSTED"`; blank or `"ALL"` = both |
| `Machine` | Optional | exact equipment name; blank = all |
| `Dept` | Optional | dept code e.g. `"00"` |
| `Operator` | Optional | cieTrade.net user name of operator |
| `FinishedProduct` | Optional | output product Grade ID; populates `FinishedProduct` in response |

Response fields: `JobNo`, `JobDate`, `Warehouse`, `WarehouseStatus`, `Machine`, `Status` (`Posted`/`Work`), `PostDate` (null if unposted), `Operator`, `StartTime`, `EndTime`, `ElapsedTime` (null if not recorded), `Department`, `UOM`, `Expenses`, `InputQty`, `OutputQty`, `YieldLossQty` (these three are weights in pounds), `Description`, `InputValue`, `OutputValue`, `UserDefined1`, `UserDefined2`, `FinishedProduct` (blank unless filter used), `InputUnits`, `OutputUnits`.

## GET /ListOrders  (Order Header)

Article: /article/1502-order-header. One row per sales or purchase order header (no line items).
Example: `https://api.cietrade.net/ListOrders?UserID=Example@cietrade.com&DateFrom=1/15/2025&DateTo=6/15/2025&Account=daktest234&Source=PO&Dept=ALLEGHENY&OrderNumber=102593&Status=open&SalesRep&TradeType&ReportUOM=L`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `Source` | **Required** | Accepts `"PO/SO"` (`PO` = purchase order, `SO` = sales order). If missing: kickback message `"Source is a required field"` |
| `Location` | Optional | (doc text copied from dispatch) location; cannot be used without `Account` |
| `DateFrom` | Optional | start of ORDER date range; default 30 days before today |
| `DateTo` | Optional | end of ORDER date range; default today |
| `Account` | Optional | account name or ID |
| `Status` | Optional | `"WORK"`, `"REVIEW"`, `"OPEN"`, `"CLOSED"`, `"CANCELED"` |
| `OrderNumber` | Optional | specific order number |
| `Dept` | Optional | dept code or short name |
| `TradeType` | Optional | trade type as defined in settings; default all |
| `SalesRep` | Optional | sales rep name; default ALL |
| `ReportUOM` | (not marked) | UOM for "total weight" of order; default `"Straight Tons"`, `ST` (example passes `L`) |

Response fields: `order_source` (`PO`/`SO`), `order_number`, `account_name`, `account_id`, `department`, `status`, `order_type` (e.g. `Spot`), `trade_type`, `order_date`, `delivery_date`, `expiration_date`, `billing_address`, `order_location` (shipping point/destination address), `terms` (shipping terms e.g. `CIF`), `payment_terms`, `ship_via`, `destination_port`, `product`, `sales_rep`, `hc_order_value`, `home_currency`, `order_value`, `order_currency`, `order_quantity`, `unit_of_measure`, `ref_number` (customer reference), `shipping_remarks`, `instructions`, `UDF1`, `UDF2`.

## GET /ListOrderDetails  (Order Detail)

Article: /article/1507-order-detail. One row per product line on a PO/SO (quantity, price, extension, attributes).
Example: `https://api.cietrade.net/ListOrderDetails?UserID=Example@cietrade.com&DateFrom=1/15/2025&DateTo=6/15/2025&Account=84698&Source=PO&Dept=ALLEGHENY&OrderNumber=102607&Status=open`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `Source` | **Required** | `"PO/SO"`; missing -> `"Source is a required field"` |
| `Location` | Optional | cannot be used without `Account` |
| `DateFrom` | Optional | ORDER date; default 30 days before today |
| `DateTo` | Optional | default today |
| `Account` | Optional | account name or ID |
| `Status` | Optional | `"WORK"`, `"REVIEW"`, `"OPEN"`, `"CLOSED"`, `"CANCELED"` |
| `OrderNumber` | Optional | specific order |
| `Dept` | Optional | dept code or short name |
| `TradeType` | Optional | default all |
| `SalesRep` | Optional | default ALL |

Response fields: `order_source`, `order_number`, `grade_name`, `grade_alternate_name`, `specifications`, `weight` (qty ordered), `weight_uom`, `price`, `price_uom`, `hc_amount`, `home_currency`, `amount` (extension), `amount_currency`, `unit_type`, `unit_count`, `color`, `property_set`, `roll_basis_weight`, `roll_basis_weight_uom`, `roll_caliper`, `roll_width`, `roll_width_uom`, `roll_diameter`, `roll_diameter_uom`, `roll_core_size`, `roll_core_size_uom`, `roll_linear_length`, `roll_linear_length_uom`, `plastic_type`, `plastic_form`, `plastic_melt`, `UDF1`..`UDF5`. (No order detail ID field is shown; worksheet details reference `SODetailID`/`PODetailID`.)

## GET /VoucherInquiry  (Payment Vouchers)

Article: /article/1557-paymentvouchers. A/P vouchers (bills) created by posting expense accruals from the AP Ledger to accounting.
Example: `https://api.cietrade.net/VoucherInquiry?UserID=Example@gmail.com&DateFrom=9/1/2025&DateTo=9/18/2025&DateType=INVOICE&Vendor=WASTECO&Dept=""&Status=Posted&APBatchNo=""` (example passes `Status`, not in table)

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Required (as marked; text says "Defaults t today's date") | |
| `DateType` | Optional | `"POST"` (posted voucher date), `"INVOICE"` (voucher invoice date), `"RECORD"` (record date) |
| `Dept` | Optional | dept ID e.g. `"00"` or short name |
| `Vendor` | Optional | name or vendor ID |
| `APBatchno` | Optional | cieTrade internal voucher number |

Response fields: `AP-Batch#`, `Vendor Name`, `Record Date`, `Posting Date`, `Invoice Date`, `Invoice No`, `Payment Terms`, `Payment Amount`, `Currency`, `HC Amount`, `PostedInAccting` (`1`/`0`), `User Name`, `Paid` (`1`/`0`), `Comment`.

## GET /PostedPayables  (Posted Payables / Expenses Payable Details)

Article: /article/1558-expenses-payable-details. One row per posted payable (material purchase or expense) tied to a worksheet/load.
Example: `https://api.cietrade.net/PostedPayables?UserID=Example@gmail.com&DateFrom=1/1/2025&DateTo=6/12/2025&Vendor=black trucks&Dept=""&DateType=POST&InvoiceNo=""`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Optional | default today |
| `DateType` | Optional | `POST` (posting date) or `INVOICE` (invoice date); default `POST` |
| `Vendor` | Optional | short name or vendor ID; blank = all |
| `Dept` | Optional | dept ID or short name; blank = all |
| `InvoiceNo` | (not marked) | vendor invoice number |

Response fields: `Invoice Date`, `Posted Date`, `AP Batch No`, `Invoice No`, `Vendor`, `Wks No` (worksheet), `Wks Invoice Date`, `Wks Shipping Date`, `Payment Terms`, `Due Date`, `Amount`, `Currency`, `{HC} Amount` (home currency; key literally includes braces), `GL Account`.

## GET /SystemLog  (System Log)

Article: /article/1432-system-log. User activity / record-change log. Limited to first 10,000 records per request.
Example: `https://api.cietrade.net/SystemLog?UserID=Example@gmail.com&DateFrom=7/10/2024&DateTo=7/12/2024&Object=ACCESS&User=dakota`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateFrom` | Required | start of log date range |
| `DateTo` | Optional | default today |
| `Object` | Optional | logging object type, e.g. `"ACCESS"`, `"BOOKING"`, `"BUYSELL"`, `"BILLSHEET"` |
| `User` | Optional | login ID that created the entry |

Response fields: `username`, `log_date`, `log_time`, `Object`, `Type` (e.g. `LOGIN`/`LOGOUT`), `Comment`, `ObjStatus`.

## GET /ListServices  (Service Records)

Article: /article/1433-service-records. One row per service/equipment at a customer location serviced by a provider on a schedule. Limited to first 75,000 rows; ordered by last updated, most recent first.
Example: `https://api.cietrade.net/ListServices?UserID=Example@gmail.com&ServiceID=813&CustomerNm=daktest234&VendorNm=black trucks&LocationNm=new location`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `ServiceID` | Optional | specific service ID; blank = ALL |
| `CustomerNm` | Optional | exact company name, Account ID, or blank |
| `LocationNm` | Optional | location; cannot be used without `CustomerNm` |
| `VendorNm` | Optional | hauler/vendor exact name or account ID |
| `Status` | Optional | `"Active"` or `"Inactive"`; blank = both |
| `AsOf` | Optional | last-updated on/after date; other filters ignored |

Response fields: `serviceNo`, `customer`, `location`, `department`, `equipment_type`, `site_reference`, `commodity_name`, `vendor`, `schedule` (e.g. `Weekly`), `interval`, `days` (e.g. `T,W,F`), `daily_interval`, `times_per_day`, `monthly_interval`, `job_type`, `start_date` (`mm-dd-yy`), `equipment_count`, `expiration_date`, `pickups_per_month`, `description` (schedule text), `UDF_1`..`UDF_4`, `Status`, `Receiver`, `receiver_location`, `Request_Sent_To`, `Dispatch_Notes`, `Questions_To_Ask`, `Bin_SensorID`, `Bin_ID`, `LastUpdated`.

## GET /ListServiceExpenses  (Service Record Expenses)

Article: /article/1434-service-record-expenses. One row per predefined charge + matching hauler expense on a service record. Limited to first 50,000 rows; ordered by last updated desc.
Example: `https://api.cietrade.net/ListServiceExpenses?UserID=Example@gmail.com&ServiceID=813&CustomerNm=daktest234&ExpenseVendorNm=BestBuy`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `ServiceID` | Optional | blank = ALL |
| `CustomerNm` | Optional | customer name or counterparty ID |
| `ExpenseVendorNm` | Optional | hauler/provider name or counterparty ID |
| `AsOf` | Optional | last-updated on/after date; other filters ignored |

Response fields: `serviceNo` (parent service record), `charge_description`, `customer`, `vendor`, `ExpGLAcNo`, `expense_price`, `expense_UOM`, `SalesGLAcNo`, `job_type`, `chargeback_price`, `Chargeback_UOM`, `material`, `LastUpdated`.

## GET /GetServiceRecordFull  (Service Record Full - Get)

Article: /article/1681-service-record-full-get. Service records with their charge lines nested under `expenses`. Limited to 75,000 rows; ordered by last updated desc. `AsOf` matches when the record OR any charge line changed.
Example: `https://api.cietrade.net/GetServiceRecordFull?UserID=Example@gmail.com&ServiceID=813`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `ServiceID` | Optional | blank = ALL |
| `CustomerNm` | Optional | name or counterparty ID |
| `LocationNm` | Optional | service location |
| `VendorNm` | Optional | hauler/provider name or counterparty ID |
| `Status` | Optional | `Active` or `Inactive`; blank = ALL |
| `AsOf` | Optional | last-updated on/after date; other filters ignored |

Response fields (record): `serviceNo`, `customer`, `location`, `department`, `equipment_type`, `equipment_no`, `equipment_count`, `site_reference`, `commodity_name`, `vendor`, `schedule`, `weekly_interval`, `days`, `daily_interval`, `times_per_day`, `monthly_interval`, `job_type`, `start_date`, `expiration_date`, `pickups_per_month`, `weight_per_yard`, `target_weight`, `target_weight_uom`, `create_service_jobs`, `receiver`, `receiver_location`, `default_request_party`, `request_sent_to`, `instructions`, `questions_to_ask`, `billingsheet_notes`, `description`, `UDF_1`..`UDF_5`, `reason_code`, `reason_description`, `Status`, `Bin_SensorID`, `Bin_ID`, `LastUpdated`, `expenses[]`.
`expenses[]` fields: `expense_id` (charge line ID, needed for updates; not returned by ListServiceExpenses), `charge_description`, `vendor`, `expense_account`, `expense_price`, `expense_uom`, `chargeback_account`, `chargeback_price`, `chargeback_uom`, `apply_to_service` (`(ANY)` or job type), `material`, `taxable`, `recurring`, `advance_bill`, `apply_fuel_surcharge`, `is_target_overweight_charge`, `benchmark_weight`, `benchmark_uom`, `LastUpdated`.

## POST /CreateServiceRecord  (Service Record Full - Create) - write, summary only

Article: /article/1682-service-record-full-create. JSON body; required `Customer`, `ServiceLocation`, `EquipmentType`, `ServiceVendorName`, `Schedule` (`On Call`/`Daily`/`Weekly`/`Monthly`), `StartDate`, plus schedule-specific fields; optional `Expenses[]` charge lines and `"ValidateOnly": true` dry-run.
Returns new service record number as string (e.g. `"104627"`), or `ERRORS: [Field]: ...`; duplicates (same Customer/Location/EquipmentType active) rejected.

## PUT /UpdateServiceRecord  (Service Record Full - Update) - write, summary only

Article: /article/1683-service-record-full-update. JSON body; `ServiceID` required, only sent fields change; charge lines addressed by `ExpenseID` (`IsDelete: true` removes; no `ExpenseID` adds).
Returns updated Service ID as string (e.g. `"813"`) or `ERRORS: [ServiceID]: Service record ... was not found.`

## GET /TradingInquiry  (Trading Inquiry)

Article: /article/1424-tradinginquiry. One row per product/grade detail line on a receiving, shipping or brokerage worksheet, with weights, prices, extensions, logistics refs, partners and margin. Note: by design does not tie out with cieTrade gross profit reports.
Example: `https://api.cietrade.net/TradingInquiry?UserID=ExampleUser@gmail.com&DateType=SHIP&DateFrom=07/01/2024&DateTo=07/02/2024&Dept=REDHOOK&TradeType=METAL&Customer=daktest234&Supplier=""&InvType=SHIP` (example uses `InvType`; table documents `InventoryType`; `TradeType` appears in example but not in table)

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateType` | Optional | `POST` = Invoice/Post-Date, `SHIP` = Ship-Date, `ASOF` = all worksheets with Invoice Date on/after `DateFrom` |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Optional | default today |
| `Dept` | Optional | dept code or short name |
| `Customer` | Optional | exact customer name or account ID; blank = all |
| `CustLoc` | Optional | customer location name; blank = all |
| `Supplier` | Optional | exact company name or account ID; blank = all |
| `SupplierLoc` | Optional | supplier location name; blank = all |
| `InventoryType` | Optional | `RCV` = receiving only, `SHIP` = shipping only; blank = all worksheets |
| `Status` | Optional | `POSTED`, `UNPOSTED`, `ALL`; default POSTED only. All options exclude cancelled |

Response fields: `WorksheetNo`, `Detailid` (line ID), `Department`, `TradeType`, `Status` (e.g. `WORK`), `ShippingDt`, `InvoiceDt`, `SoOrderDt`, `PoOrderDt`, `Customer`, `CustomerAddress`, `ShipFromID` (ship-from location/warehouse), `RelNo` (release no), `ContainerNo`, `Supplier` (`(INVENTORY)` for inventory sales), `SupplierAddress`, `ShipTo`, `PickupNo`, `BookingNo`, `SoDestPort`, `PoDestPort`, `ETA`, `ProductCat`, `ProductGrp`, `ProductName`, `Specifications`, `InvoiceDesc`, `GradeID`, `Units`, `UnitType`, `WksWeightLBS`, `SWeight` (sale weight), `SWeightUOM`, `PWeight` (purchase weight), `PWeightUOM`, `SWeightLBS`, `WksSales`, `WksChgBack`, `WksExp`, `WksPurchases`, `WksSAdj`, `WksPAdj` (worksheet-level totals), `Factor`, `SPO` (sales order ref), `SCurrencyCd`, `SPrice`, `SPriceUOM`, `SAmount`, `PO` (purchase order ref), `PCurrencyCd`, `PPrice`, `PPriceUOM`, `PAmount`, `ChargeBacks`, `FreightExp`, `Expenses`, `SAdjs`, `PAdjs`, `Interest`, `BankFees`, `Discount`, `Margin`, `TotalSale`, `TotalCost`, `GrossProfit`, `NetProfit`, `Sell-Rep`, `Buy-Rep`, `IsAgency` (`0`/`1`), `IsNegSales` (`0`/`1`), `TranSrc` (e.g. `INV`), `UserDefined1`..`UserDefined3`, `OwnerName`, `POShippingTerms`, `POShipVia`, `POPaymentTerms`, `SOShippingTerms`, `SOShipVia`, `SOPaymentTerms`, `CustID`, `SupplierID` (`(INV)` for inventory), `EquipNo`, `GrossWt`, `TareWt`, `NetWt`, `Logistics` (e.g. `SHIPPED`).

## GET /ListWorksheets  (Worksheet header)

Article: /article/1423-api-worksheet. One row per worksheet (load): received load / Purchase Receipt "PR", brokerage buy/sell, or inventory sale/shipment; summary financials + logistics, no line items (use TradingInquiry / ListWorksheetDetails). Inventory sales have Supplier `(INV)`; inventory receipts have Customer `(INV)`. If no status filter, only INVOICED or POSTED worksheets returned.
Example: `https://api.cietrade.net/ListWorksheets?UserID=ExampleUser@gmail.com&DateFrom=7/1/2024&DateTo=7/3/2024&TradeType=METAL&Status=Work&DateType=SHIP&Customer=daktest234&Supplier=""&InvType=SHIP&Warehouse=PERKIOMEN&CustLoc=new location&SuppLoc=""&Dept=REDHOOK` (example uses `InvType`/`SuppLoc`; table documents `InventoryType`/`SupplierLoc`)

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateType` | Optional | `INV` = Invoice-Date, `SHIP` = Ship-Date, `ASOF` = all worksheets with Invoice Date on/after `DateFrom` |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Optional | default today |
| `Customer` | Optional | exact customer name or account ID; blank = all |
| `CustLoc` | Optional | ship-to / receiving location of customer; blank = all |
| `Supplier` | Optional | exact company name or account ID; blank = all |
| `SupplierLoc` | Optional | shipping point / pickup location of supplier; blank = all |
| `InventoryType` | Optional | `RCV` = receiving only, `SHIP` = shipping only; blank = all |
| `Warehouse` | Optional | warehouse name (used with `InventoryType`) |
| `Status` | Optional | listed twice: (a) "blank = ALL status types"; (b) "default INVOICE or POSTED; options `POSTED`, `UNPOSTED`, `ALL`; cancelled always excluded" |
| `Dept` | Optional | dept code or short name |
| `TradeType` | Optional | worksheet trade type; blank = all |
| `WorksheetUDFx` | Optional | x = 1..5, e.g. `WorksheetUDF2=value`; exact match on that user-defined field |
| `OrderNo` | (not marked) | worksheets associated with a specific SO/PO |

Response fields: `WorksheetNo`, `Department`, `TradeType`, `ShippingDate`, `status`, `PostDate`, `Customer`, `ShipToLocation`, `Supplier`, `ShipFromLocation`, `FreightCarrier`, `FreightRate`, `ReleaseNumber`, `PickupNumber`, `BookingNumber`, `ShippingStatus` (e.g. `SETUP`), `Sales`, `Chargebacks`, `TotalSale`, `Purchases`, `Expenses`, `TotalCosts`, `SalesAdj`, `PurchaseAdj`, `GrossProfit`, `SalesTaxAmnt`, `SalesCurrency`, `ExchangeRate`, `PurchaseFxRate`, `FxInvoiceAmount`, `FxSalesTaxAmt`, `PurchaseCurrency`, `FxPurchases`, `WeightInLbs`, `GradeRef`, `GradeRefID`, `WorksheetUDF1`..`WorksheetUDF5`, `SalesRep`, `SalesCommRate`, `SalesCommRateUOM`, `salesCommValue`, `BuyRep`, `BuyCommRate`, `BuyCommRateUOM`, `BuyCommValue`, `PoNo`, `SoNo`.

## GET /ListWorksheetDetails  (Worksheet Detail)

Article: /article/1506-worksheet-detail. One row per commodity/grade line on a worksheet (qty, pricing, amounts, attributes, SO/PO references).
Example: `https://api.cietrade.net/ListWorksheetDetails?UserID=Example@cieTrade.com&DateFrom=&DateTo=&TradeType=&Status=&DateType=&Customer=&Supplier=&InvType=&Warehouse=&CustLoc=&SuppLoc=&Dept=&WorksheetNo=514243`

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateType` | Optional | `INV`, `SHIP`, `ASOF` (ASOF here = shipping date on/after `DateFrom`) |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Optional | default today |
| `Customer` | Optional | name or account ID |
| `CustLoc` | Optional | customer location |
| `Supplier` | Optional | name or account ID |
| `SupplierLoc` | Optional | supplier location |
| `InventoryType` | Optional | `RCV` or `SHIP`; blank = all |
| `Warehouse` | Optional | warehouse name |
| `Status` | Optional | blank = ALL status types |
| `Dept` | Optional | dept code or short name |
| `TradeType` | Optional | blank = all |
| `WorksheetUDFx` | Optional | x = 1..5 |
| `OrderNo` | (not marked) | items associated with SO/PO |
| `OrderDetailID` | (not marked) | items associated with a specific SO/PO detail item |
| `WorksheetNo` | (not marked) | returns details for that worksheet and overrides all other parameters |

Response fields: `WorksheetNo`, `grade_name`, `alternate_name`, `sale_weight`, `sale_weight_uom`, `sale_price`, `sale_price_uom`, `purchase_weight`, `purchase_weight_uom`, `purchase_price`, `purchase_price_uom`, `sale_amount`, `sale_currency`, `purchase_amount`, `purchase_currency`, `hc_sale_amount`, `hc_purchase_amount`, `home_currency`, `specifications`, `unit_type`, `unit_count`, `color`, `property_set`, `roll_basis_weight`, `roll_basis_weight_uom`, `roll_caliper`, `roll_width`, `roll_width_uom`, `roll_diameter`, `roll_diameter_uom`, `roll_core_size`, `roll_core_size_uom`, `roll_linear_length`, `roll_linear_length_uom`, `plastic_type`, `plastic_form`, `plastic_melt`, `UDF1`..`UDF5`, `SODetailID`, `SoNo`, `PODetailID`, `PoNo`.

## GET /ListWorksheetExpenses  (Worksheet Expense)

Article: /article/1522-worksheet-expense. One row per worksheet expense (freight, commission, other cost) and/or customer chargeback. On PR (inventory receipt) worksheets the chargeback columns mirror the expense columns and can be ignored.
Example: `https://api.cietrade.net/ListWorksheetExpenses?UserID=dakota&DateFrom=6/1/2025&DateTo=6/20/2025&TradeType=&Status=all&DateType=&Customer=&Supplier=&InvType=&Warehouse=&CustLoc=''&SuppLoc=''&Dept=''&WorksheetNo=&ExpenseVendor=''` (example passes `ExpenseVendor`, not in table)

| Parameter | Required | Values / notes |
|---|---|---|
| `UserID` | Required | |
| `DateType` | Optional | `INV`, `SHIP`, `ASOF` (shipping date on/after `DateFrom`) |
| `DateFrom` | Optional | default 30 days before today |
| `DateTo` | Optional | default today |
| `Customer` | Optional | name or account ID |
| `CustLoc` | Optional | |
| `Supplier` | Optional | name or account ID |
| `SupplierLoc` | Optional | |
| `InventoryType` | Optional | `RCV` or `SHIP`; blank = all |
| `Warehouse` | Optional | |
| `Status` | Optional | `Posted`, `Unposted`, `All`; blank = ALL |
| `Dept` | Optional | |
| `TradeType` | Optional | |
| `WorksheetUDFx` | Optional | x = 1..5 |
| `WorksheetNo` | Optional | overrides all other parameters |

Response fields: `item_id` (expense line ID), `WorksheetNo` (e.g. `PR-10628`), `expense_vendor_id`, `expense_vendor`, `expense_description`, `expense_price`, `expense_currency`, `expense_price_uom`, `hc_expense_total_amount`, `expense_total_amount`, `chargeback_price`, `chargeback_currency`, `chargeback_price_uom`, `hc_chargeback_total_amount`, `chargeback_total_amount`, `expense_account` (GL), `sales_account` (GL).

---

## Doc inconsistencies worth knowing (verbatim discrepancies between example URLs and parameter tables)

- Example URLs use `InvType`, `SuppLoc`, `SearchType`, `Type`, `AccountName`, `ExpenseVendor` where the tables document `InventoryType`, `SupplierLoc`, `DateType`, `DateType`, `Account`, (undocumented). Prefer the table names; test both if a filter appears ignored.
- `ListOrders`/`ListOrderDetails` `Location` parameter description is copy-pasted from Dispatch Jobs.
- `ListWorksheets` lists `Status` twice with different semantics.
- `VoucherInquiry` marks `DateTo` Required but says it defaults to today; example passes an undocumented `Status=Posted`.
- `ListInventory` response key `posed_date` (typo for posted date) is as printed.
- `PostedPayables` response key `{HC} Amount` includes braces as printed.
