# Internship Task: Building a Data Warehouse from Adventureworks Data
## UPDATED VERSION - Complete & Detailed (WITH ERROR HANDLING)

## Objective
The goal of this task is to build a comprehensive data warehouse using Adventureworks data as the source in PostgreSQL. Interns will design fact and dimension tables, implement these in ClickHouse, and build ETL pipelines using Apache Airflow to load and aggregate data for analysis. The pipeline must include robust error handling, data quality checks, and comprehensive error tracking mechanisms.

---

## Task Overview

### Step 1: Load Adventureworks Data into PostgreSQL
- Obtain a full Adventureworks sample database loaded into PostgreSQL.
- Familiarize yourselves with the schema and data relationships.
- Url: https://github.com/morenoh149/postgresDBSamples/tree/master/adventureworks

---

### Step 2: Design Fact and Dimension Tables for Data Warehouse

#### Fact Tables (Quantitative Metrics)

**Core Fact Tables:**

- **FactSales**
  - Metrics: Sales Revenue, Quantity Sold, Discount Amount, Number of Transactions
  - Dimensions: DimDate (SalesDateKey), DimCustomer, DimProduct, DimStore, DimEmployee
  - Grain: One row per order detail line item
  - Partitioning: By SalesDateKey

- **FactPurchases**
  - Metrics: Purchase Amount, Purchase Quantity, Discounts, Unit Cost
  - Dimensions: DimDate (PurchaseDateKey), DimProduct, DimVendor
  - Grain: One row per purchase order line item
  - Partitioning: By PurchaseDateKey

- **FactInventory**
  - Metrics: Quantity on Hand, Stock Aging (days), Reorder Levels, Safety Stock
  - Dimensions: DimDate (InventoryDateKey), DimProduct, DimStore, DimWarehouse
  - Grain: One row per product per warehouse per day
  - Partitioning: By InventoryDateKey
  - Note: This is a **factless fact table** with time-series snapshots

- **FactProduction**
  - Metrics: Units Produced, Production Time (hours), Scrap Rate (%), Defect Count
  - Dimensions: DimDate (ProductionDateKey), DimProduct, DimEmployee (supervisor)
  - Grain: One row per production run/batch
  - Partitioning: By ProductionDateKey

- **FactEmployeeSales**
  - Metrics: Sales Amount by Employee, Sales Target vs. Actual, Customer Contacts Count
  - Dimensions: DimDate (SalesDateKey), DimEmployee, DimStore, DimSalesTerritory
  - Grain: One row per employee per day
  - Partitioning: By SalesDateKey

- **FactCustomerFeedback**
  - Metrics: Feedback Scores (1-5), Complaint Counts, Resolution Times (hours), CSAT Score
  - Dimensions: DimDate (FeedbackDateKey), DimCustomer, DimEmployee (handler), DimFeedbackCategory
  - Grain: One row per feedback submission
  - Partitioning: By FeedbackDateKey

- **FactPromotionResponse**
  - Metrics: Sales During Campaign, Discount Usage Count, Customer Uptake Rate (%), Promotion ROI
  - Dimensions: DimDate (PromotionDateKey), DimProduct, DimStore, DimPromotion
  - Grain: One row per product per promotion per store per day
  - Partitioning: By PromotionDateKey

- **FactFinance**
  - Metrics: Invoice Amounts, Payment Delays (days), Credit Usage (%), Interest Charges
  - Dimensions: DimDate (InvoiceDateKey), DimCustomer, DimStore, DimFinanceCategory
  - Grain: One row per invoice
  - Partitioning: By InvoiceDateKey

- **FactReturns**
  - Metrics: Returned Quantity, Refund Amount, Return Reasons, Restocking Fee
  - Dimensions: DimDate (ReturnDateKey), DimProduct, DimCustomer, DimStore, DimReturnReason
  - Grain: One row per return line item
  - Partitioning: By ReturnDateKey

#### Aggregated Fact Tables

- **agg_daily_sales**
  - Aggregation: Daily sums of sales revenue, quantity, discount amount, transaction count
  - Dimensions: DimDate (SalesDateKey), DimStore, DimProductCategory
  - Grain: One row per store per product category per day
  - Update Frequency: Daily (post-midnight)

- **agg_weekly_sales**
  - Aggregation: Weekly sales summary by product category and region (SUM, AVG, MIN, MAX)
  - Dimensions: DimDate (WeekStartDateKey), DimRegion, DimProductCategory
  - Grain: One row per region per product category per week
  - Update Frequency: Weekly (Sundays)

- **agg_monthly_sales**
  - Aggregation: Monthly sales totals by customer segments (SUM revenue, AVG order value, distinct customer count)
  - Dimensions: DimDate (MonthStartDateKey), DimCustomerSegment, DimRegion
  - Grain: One row per customer segment per region per month
  - Update Frequency: Monthly (1st of next month)

- **agg_daily_inventory** (NEW)
  - Aggregation: Average inventory value by warehouse, product category, and aging tier
  - Dimensions: DimDate (InventoryDateKey), DimWarehouse, DimProductCategory, DimAgingTier
  - Grain: One row per warehouse per product category per aging tier per day
  - Update Frequency: Daily

- **agg_monthly_product_performance** (NEW)
  - Aggregation: Product performance metrics (revenue, units sold, returns rate, avg rating)
  - Dimensions: DimDate (MonthStartDateKey), DimProduct, DimStore
  - Grain: One row per product per store per month
  - Update Frequency: Monthly

- **agg_regional_sales** (NEW)
  - Aggregation: Regional sales summary with growth rate calculations
  - Dimensions: DimDate (MonthStartDateKey), DimRegion, DimSalesTerritory
  - Grain: One row per region per territory per month
  - Update Frequency: Monthly

#### Dimension Tables (Contextual Attributes)

**DimDate** (SCD Type 1 - Static)
- Attributes:
  - DateKey (INTEGER PRIMARY KEY)
  - FullDate (DATE)
  - Year, Quarter, Month, MonthName, Week, DayOfWeek, DayName
  - DayOfMonth, DayOfYear, WeekOfYear
  - IsWeekend (BOOLEAN)
  - IsHoliday (BOOLEAN)
  - HolidayName (VARCHAR)
  - FiscalYear, FiscalQuarter, FiscalMonth
  - Season (VARCHAR)
- Note: Static reference table, no versioning needed

**DimCustomer** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - CustomerKey (INTEGER PRIMARY KEY - Surrogate Key)
  - CustomerID (INTEGER - Natural Key from source)
  - CustomerName (VARCHAR)
  - Email (VARCHAR)
  - Phone (VARCHAR)
  - City (VARCHAR)
  - StateProvince (VARCHAR)
  - Country (VARCHAR)
  - PostalCode (VARCHAR)
  - CustomerSegment (VARCHAR) - e.g., "Premium", "Standard", "Budget"
  - CustomerType (VARCHAR) - e.g., "Individual", "Corporate"
  - AccountStatus (VARCHAR) - e.g., "Active", "Inactive", "Suspended"
  - CreditLimit (DECIMAL(18,2))
  - AnnualIncome (DECIMAL(18,2))
  - YearsSinceFirstPurchase (INTEGER)
  - **ValidFromDate (DATE)** - When this version became active
  - **ValidToDate (DATE)** - When this version expired (NULL if current)
  - **IsCurrent (BOOLEAN)** - TRUE if current version, FALSE if historical
  - **SourceUpdateDate (DATE)** - When source system was updated
  - **EffectiveStartDate (DATE)** - Effective date in the warehouse
  - **EffectiveEndDate (DATE)** - Effective end date in the warehouse
- Merge Strategy: UPSERT with change detection on (Name, Email, City, Country, Segment, Status)

**DimProduct** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - ProductKey (INTEGER PRIMARY KEY - Surrogate Key)
  - ProductID (INTEGER - Natural Key from source)
  - ProductName (VARCHAR)
  - SKU (VARCHAR)
  - Category (VARCHAR)
  - SubCategory (VARCHAR)
  - Brand (VARCHAR)
  - ListPrice (DECIMAL(18,2))
  - Cost (DECIMAL(18,2))
  - ProductStatus (VARCHAR) - e.g., "Active", "Discontinued", "Coming Soon"
  - Color (VARCHAR)
  - Size (VARCHAR)
  - Weight (DECIMAL(10,3))
  - **ValidFromDate (DATE)** - When this price/category version became active
  - **ValidToDate (DATE)** - When this version expired (NULL if current)
  - **IsCurrent (BOOLEAN)** - TRUE if current version
  - **SourceUpdateDate (DATE)**
  - **EffectiveStartDate (DATE)**
  - **EffectiveEndDate (DATE)**
- Merge Strategy: UPSERT with change detection on (ListPrice, Cost, Category, Status)

**DimStore** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - StoreKey (INTEGER PRIMARY KEY - Surrogate Key)
  - StoreID (INTEGER - Natural Key)
  - StoreName (VARCHAR)
  - StoreNumber (INTEGER)
  - Address (VARCHAR)
  - City (VARCHAR)
  - StateProvince (VARCHAR)
  - Country (VARCHAR)
  - PostalCode (VARCHAR)
  - Region (VARCHAR)
  - Territory (VARCHAR)
  - StoreType (VARCHAR) - e.g., "Retail", "Warehouse", "Outlet"
  - StoreStatus (VARCHAR) - e.g., "Open", "Closed", "Remodeling"
  - ManagerName (VARCHAR)
  - OpeningDate (DATE)
  - SquareFootage (INTEGER)
  - **ValidFromDate (DATE)**
  - **ValidToDate (DATE)**
  - **IsCurrent (BOOLEAN)**
  - **SourceUpdateDate (DATE)**
- Merge Strategy: UPSERT with change detection on (Address, Region, Territory, Manager, Status)

**DimEmployee** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - EmployeeKey (INTEGER PRIMARY KEY - Surrogate Key)
  - EmployeeID (INTEGER - Natural Key)
  - EmployeeName (VARCHAR)
  - JobTitle (VARCHAR)
  - Department (VARCHAR)
  - ReportingManagerKey (INTEGER - Self-referencing to DimEmployee)
  - HireDate (DATE)
  - EmployeeStatus (VARCHAR) - e.g., "Active", "On Leave", "Terminated"
  - Region (VARCHAR)
  - Territory (VARCHAR)
  - SalesQuota (DECIMAL(18,2))
  - **ValidFromDate (DATE)**
  - **ValidToDate (DATE)**
  - **IsCurrent (BOOLEAN)**
  - **SourceUpdateDate (DATE)**
- Merge Strategy: UPSERT with change detection on (JobTitle, Department, Region, Territory, Quota)

**DimPromotion** (SCD Type 1 or Type 2 - Configurable per business rules)
- Attributes (SCD Type 1 recommendation):
  - PromotionKey (INTEGER PRIMARY KEY)
  - PromotionID (INTEGER - Natural Key)
  - PromotionName (VARCHAR)
  - PromotionDescription (TEXT)
  - PromotionType (VARCHAR) - e.g., "Discount", "BOGO", "Bundle"
  - DiscountPercentage (DECIMAL(5,2))
  - DiscountAmount (DECIMAL(18,2))
  - StartDate (DATE)
  - EndDate (DATE)
  - IsActive (BOOLEAN)
  - PromotionStatus (VARCHAR)
  - CampaignID (INTEGER)
  - TargetProductKey (INTEGER FK to DimProduct, NULLABLE for store-wide promos)
  - TargetCustomerSegment (VARCHAR, NULLABLE)
- If tracking promotion changes (SCD Type 2): Add ValidFromDate, ValidToDate, IsCurrent

**DimVendor** (SCD Type 2 - Slowly Changing with History)
- Attributes:
  - VendorKey (INTEGER PRIMARY KEY - Surrogate Key)
  - VendorID (INTEGER - Natural Key)
  - VendorName (VARCHAR)
  - ContactPerson (VARCHAR)
  - Email (VARCHAR)
  - Phone (VARCHAR)
  - Address (VARCHAR)
  - City (VARCHAR)
  - Country (VARCHAR)
  - VendorRating (DECIMAL(3,2)) - 1.0 to 5.0
  - OnTimeDeliveryRate (DECIMAL(5,2)) - Percentage
  - QualityScore (DECIMAL(5,2))
  - PaymentTerms (VARCHAR)
  - VendorStatus (VARCHAR) - e.g., "Active", "Inactive", "Preferred"
  - **ValidFromDate (DATE)**
  - **ValidToDate (DATE)**
  - **IsCurrent (BOOLEAN)**
  - **SourceUpdateDate (DATE)**
- Merge Strategy: UPSERT with change detection on (Rating, OnTimeDeliveryRate, QualityScore, Status)

**DimFeedbackCategory** (SCD Type 1 - Static)
- Attributes:
  - FeedbackCategoryKey (INTEGER PRIMARY KEY)
  - FeedbackCategoryID (INTEGER)
  - CategoryName (VARCHAR) - e.g., "Product Quality", "Delivery", "Customer Service", "Price"
  - CategoryDescription (TEXT)

**DimReturnReason** (SCD Type 1 - Static)
- Attributes:
  - ReturnReasonKey (INTEGER PRIMARY KEY)
  - ReturnReasonID (INTEGER)
  - ReturnReasonName (VARCHAR) - e.g., "Defective", "Wrong Item", "Changed Mind", "Damaged"
  - ReturnReasonDescription (TEXT)

**DimWarehouse** (SCD Type 2 - Optional, if separate from DimStore)
- Attributes:
  - WarehouseKey (INTEGER PRIMARY KEY)
  - WarehouseID (INTEGER)
  - WarehouseName (VARCHAR)
  - Location (VARCHAR)
  - WarehouseType (VARCHAR) - e.g., "Distribution Center", "Regional Hub"
  - ManagerKey (INTEGER FK to DimEmployee)
  - **ValidFromDate (DATE)**
  - **ValidToDate (DATE)**
  - **IsCurrent (BOOLEAN)**

**DimSalesTerritory** (SCD Type 1 or Type 2)
- Attributes:
  - TerritoryKey (INTEGER PRIMARY KEY)
  - TerritoryID (INTEGER)
  - TerritoryName (VARCHAR)
  - SalesRegion (VARCHAR)
  - Country (VARCHAR)
  - Manager (VARCHAR)
  - SalesTarget (DECIMAL(18,2))

**DimCustomerSegment** (SCD Type 1 - Static)
- Attributes:
  - SegmentKey (INTEGER PRIMARY KEY)
  - SegmentID (INTEGER)
  - SegmentName (VARCHAR) - e.g., "Premium", "Standard", "Budget", "VIP"
  - SegmentDescription (TEXT)
  - DiscountTierStart (DECIMAL(5,2))
  - DiscountTierEnd (DECIMAL(5,2))

**DimAgingTier** (SCD Type 1 - Static)
- Attributes:
  - AgingTierKey (INTEGER PRIMARY KEY)
  - AgingTierID (INTEGER)
  - AgingTierName (VARCHAR) - e.g., "Fresh (0-30 days)", "Aged (31-90 days)", "Very Aged (90+ days)"
  - MinAgingDays (INTEGER)
  - MaxAgingDays (INTEGER)

**DimFinanceCategory** (SCD Type 1 - Static)
- Attributes:
  - FinanceCategoryKey (INTEGER PRIMARY KEY)
  - FinanceCategoryID (INTEGER)
  - CategoryName (VARCHAR) - e.g., "Invoice", "Payment", "Credit Memo", "Adjustment"
  - CategoryDescription (TEXT)

**DimRegion** (SCD Type 1 - Static)
- Attributes:
  - RegionKey (INTEGER PRIMARY KEY)
  - RegionID (INTEGER)
  - RegionName (VARCHAR)
  - Country (VARCHAR)
  - Continent (VARCHAR)
  - TimeZone (VARCHAR)

**DimProductCategory** (SCD Type 1 - Static)
- Attributes:
  - ProductCategoryKey (INTEGER PRIMARY KEY)
  - ProductCategoryID (INTEGER)
  - CategoryName (VARCHAR)
  - CategoryDescription (TEXT)

---

### Step 3: Implement Tables in ClickHouse

#### Table Creation Requirements

**For SCD Type 1 Dimensions (Static):**
```sql
CREATE TABLE DimDate
(
    DateKey UInt32 PRIMARY KEY,
    FullDate Date,
    Year UInt16,
    ...
)
ENGINE = MergeTree()
ORDER BY (DateKey);
```

**For SCD Type 2 Dimensions (Slowly Changing):**
```sql
CREATE TABLE DimCustomer
(
    CustomerKey UInt32,
    CustomerID UInt32,
    CustomerName String,
    ...
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Nullable(Date)
)
ENGINE = ReplacingMergeTree(EffectiveEndDate)
ORDER BY (CustomerKey, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);
```

**For Fact Tables:**
```sql
CREATE TABLE FactSales
(
    SalesDateKey UInt32,
    CustomerKey UInt32,
    ProductKey UInt32,
    StoreKey UInt32,
    EmployeeKey UInt32,
    SalesAmount Decimal(18, 2),
    Quantity UInt32,
    DiscountAmount Decimal(18, 2),
    TransactionCount UInt32
)
ENGINE = MergeTree()
ORDER BY (SalesDateKey, CustomerKey, ProductKey, StoreKey)
PARTITION BY toYYYYMM(SalesDateKey);
```

**For Aggregated Fact Tables (Materialized Views):**
```sql
CREATE TABLE agg_daily_sales
(
    SalesDateKey UInt32,
    StoreKey UInt32,
    ProductCategoryKey UInt32,
    TotalRevenue Decimal(18, 2),
    TotalQuantity UInt32,
    TotalDiscount Decimal(18, 2),
    TransactionCount UInt32
)
ENGINE = SummingMergeTree((TotalRevenue, TotalQuantity, TotalDiscount, TransactionCount))
ORDER BY (SalesDateKey, StoreKey, ProductCategoryKey)
PARTITION BY toYYYYMM(SalesDateKey);
```

**Key Implementation Points:**
- Use `ReplacingMergeTree` for SCD Type 2 dimensions (maintains version history)
- Use `SummingMergeTree` for aggregated fact tables (optimizes SUM operations)
- Use `PartitionBy` clause to partition by date (monthly is typical for large tables)
- Define appropriate `ORDER BY` clause (typically dimension keys + date)
- Set `CODEC` for compression on large columns
- Use `Nullable` types only where necessary (impacts performance)

#### Data Types Mapping
- Integer Keys: `UInt32` or `UInt64`
- Amounts/Prices: `Decimal(18, 2)`
- Percentages: `Decimal(5, 2)`
- Dates: `Date` or `DateTime`
- Flags/Booleans: `UInt8`
- Strings: `String` (no length limit in ClickHouse)
- Nullable values: Use `Nullable(Type)`

#### Error Records Table (New)
```sql
CREATE TABLE error_records (
    ErrorID UInt64,
    ErrorDate DateTime,
    SourceTable String,
    RecordNaturalKey String,
    ErrorType String,
    ErrorSeverity String,
    ErrorMessage String,
    ErrorDetails String,
    FailedData String,
    ProcessingBatchID String,
    TaskName String,
    IsRecoverable UInt8,
    RetryCount UInt8 DEFAULT 0,
    LastAttemptDate DateTime,
    IsResolved UInt8 DEFAULT 0,
    ResolutionComment Nullable(String)
) ENGINE = MergeTree()
ORDER BY (ErrorDate, SourceTable, ErrorType)
PARTITION BY toYYYYMM(ErrorDate);
```

---

### Step 4: Build ETL Pipelines Using Apache Airflow

#### ETL Architecture Overview

The Airflow pipeline will be structured as follows:

```
[PostgreSQL Source] 
    ↓
[Extract Task] → [Transform Task] → [Load Dimension Task] → [Load Fact Task] → [Update Aggregates Task] → [Error Handling]
                                          ↓
                                    [SCD Type 2 Merge Logic]
                                          ↓
                                    [ClickHouse]
                                          ↓
                                    [Error Records Table]
```

#### Pipeline Tasks & Logic

**1. Extract Task** - `extract_incremental_data`
- Query PostgreSQL for data relevant to processing_date
- For dimensions: Extract all records that have changed since last run (use `LastModifiedDate`)
- For facts: Extract transactions for the processing_date
- Return extracted data as pandas DataFrames or parquet files
- Logging: Record row counts extracted for audit

**2. Data Quality Validation Task** - `validate_extracted_data`
- Validate extracted data before transformation
- Checks to perform:
  - Null checks on non-nullable columns
  - Data type validation
  - Date range validation (no future dates unless applicable)
  - Numeric range validation (no negative amounts for revenue)
  - Duplicate key detection
- Log validation results to monitoring table
- If failures detected: Log to error_records table, alert team

**3. Dimension SCD Type 2 Load Logic** - `load_dimension_scd2`
- Input: Extracted dimension changes
- Detection: Compare natural keys and attributes against existing DimCustomer in ClickHouse
- Merge Logic:
  - If customer not found: INSERT new row with IsCurrent=1, ValidFromDate=ProcessingDate, ValidToDate=NULL
  - If customer found but attributes changed: 
    - UPDATE existing row: SET ValidToDate=ProcessingDate-1, IsCurrent=0
    - INSERT new row: IsCurrent=1, ValidFromDate=ProcessingDate, ValidToDate=NULL
  - If customer found but no changes: No action
- Query for merge detection:
  ```sql
  SELECT * FROM DimCustomer 
  WHERE CustomerID IN (extracted_ids) 
  AND IsCurrent = 1
  ```
- Change comparison: Compare email, city, country, segment, status against new data
- ClickHouse INSERT/UPDATE: Use `ALTER TABLE DimCustomer UPDATE` or use Python driver for batch INSERT
- **Error Handling:** On failed record, write to error_records table with ErrorType="DimensionLoadFailed" and IsRecoverable=1

**4. Dimension SCD Type 1 Load Logic** - `load_dimension_scd1`
- Input: Extracted dimension data
- Simple UPSERT: If key exists, UPDATE all attributes; else INSERT
- No version history maintenance
- Example: DimDate, DimFeedbackCategory
- **Error Handling:** On duplicate key or data type error, log to error_records with ErrorType="DimensionValidationFailed"

**5. Fact Table Load Task** - `load_fact_tables`
- Input: Extracted fact data (FactSales, FactPurchases, etc.)
- Lookup Keys: For each fact row, retrieve foreign keys:
  - DimCustomer: Get CustomerKey from CustomerID (use IsCurrent=1)
  - DimProduct: Get ProductKey from ProductID (use IsCurrent=1)
  - DimStore: Get StoreKey from StoreID (use IsCurrent=1)
  - DimDate: Get DateKey from transaction date
- Validation:
  - Check for NULL foreign keys (failed lookups) → log as error, write to error_records
  - Check for out-of-range amounts (negative revenue) → log as warning, skip row
  - Check for future dates → log as warning, skip row
- **Error Handling Strategy:**
  - For recoverable errors (FK miss, type mismatch): Write to error_records table with IsRecoverable=1
  - For non-recoverable errors (schema mismatch, critical data loss): Stop pipeline, alert team immediately
  - Log failed records with FailedData (JSON serialized) for manual review
- Load to ClickHouse: Batch INSERT fact records with all resolved foreign keys
- Grain validation: Ensure fact table grain is maintained (e.g., one row per order line)

**6. Aggregate Update Task** - `update_aggregates`
- Input: Processed date
- Strategy 1 (Materialized View): Define as SQL query, rebuild nightly
  ```sql
  CREATE MATERIALIZED VIEW agg_daily_sales_view AS
  SELECT 
      SalesDateKey,
      StoreKey,
      ProductCategoryKey,
      SUM(SalesAmount) as TotalRevenue,
      SUM(Quantity) as TotalQuantity,
      SUM(DiscountAmount) as TotalDiscount,
      COUNT(*) as TransactionCount
  FROM FactSales
  GROUP BY SalesDateKey, StoreKey, ProductCategoryKey;
  ```
- Strategy 2 (Batch Compute): Query FactSales for previous day, aggregate, INSERT to agg_daily_sales table
  ```python
  query = """
  SELECT 
      SalesDateKey,
      StoreKey,
      ProductCategoryKey,
      SUM(SalesAmount) as TotalRevenue,
      ...
  FROM FactSales
  WHERE SalesDateKey = ?
  GROUP BY SalesDateKey, StoreKey, ProductCategoryKey
  """
  ```
- Incremental updates: Only compute for processing_date (not full recalc)
- **Error Handling:** If aggregation fails, log to error_records but continue (aggregates can be recomputed)

**7. Error Reprocessing Task** - `reprocess_recoverable_errors`
- Runs daily after main ETL completes
- Query error_records table:
  ```sql
  SELECT * FROM error_records
  WHERE IsResolved = 0 
  AND IsRecoverable = 1 
  AND RetryCount < 3 
  AND ErrorDate >= now() - INTERVAL 1 DAY
  ORDER BY LastAttemptDate ASC
  LIMIT 100;
  ```
- Attempt reprocessing with updated data lookups
- If successful: Set IsResolved=1, update ResolutionComment
- If failed: Increment RetryCount, update LastAttemptDate
- If RetryCount > 3: Flag for manual review (alert data team)

#### Error Handling & Retry Logic

**Error Scenarios & Handling:**

| Error Type | Recoverable | Action | Retry Policy |
|-----------|-------------|--------|--------------|
| FK Miss (late-arriving dims) | Yes | Log to error_records, skip row | Retry next run |
| Null PK | No | Log error, alert team | Manual fix required |
| Data Type Mismatch | Sometimes | Attempt conversion, log if failed | 3 retries |
| Duplicate PK | Yes | Skip duplicate, log | Auto-reprocess |
| Connection Timeout | Yes | Retry with exponential backoff | 3x with 60s delay |
| Out-of-Range Value | Yes | Log warning, skip row | Continue processing |
| Schema Mismatch | No | Stop pipeline, alert | Manual team intervention |

**Pseudocode for Error Handling:**
```python
def load_fact_with_error_handling(fact_data, processing_batch_id, task_name):
    error_records = []
    loaded_records = []
    
    for idx, record in fact_data.iterrows():
        try:
            # Resolve foreign keys
            customer_key = lookup_customer_key(record['CustomerID'])
            if not customer_key:
                raise ValueError(f"FK Miss: CustomerID {record['CustomerID']} not found")
            
            # Validate values
            if record['SalesAmount'] < 0:
                raise ValueError(f"Invalid revenue: {record['SalesAmount']}")
            
            # Insert to ClickHouse
            clickhouse_client.insert_one(record)
            loaded_records.append(record)
            
        except Exception as e:
            error_record = {
                'ErrorDate': datetime.now(),
                'SourceTable': 'FactSales',
                'RecordNaturalKey': f"{record['OrderID']}-{record['OrderLineID']}",
                'ErrorType': type(e).__name__,
                'ErrorMessage': str(e),
                'ErrorSeverity': determine_severity(e),
                'FailedData': json.dumps(record.to_dict()),
                'ProcessingBatchID': processing_batch_id,
                'TaskName': task_name,
                'IsRecoverable': int(is_recoverable_error(e)),
                'RetryCount': 0,
                'IsResolved': 0
            }
            error_records.append(error_record)
            
            # If non-recoverable, stop immediately
            if not error_record['IsRecoverable']:
                log.error(f"Non-recoverable error: {error_record}")
                raise
    
    # Write error records to ClickHouse
    if error_records:
        clickhouse_client.insert('error_records', error_records)
        log.warning(f"Loaded {len(loaded_records)} records, {len(error_records)} errors")
    
    return len(loaded_records), len(error_records)
```

**Retry Configuration:**
- Connection failures: 3 retries with exponential backoff (60s, 120s, 240s)
- Data validation errors: Skip row, continue processing
- Schema errors: Stop immediately, escalate

**Alerting:**
- **Critical errors** (non-recoverable, schema mismatch): Email to data-warehouse@company.com immediately
- **Warning errors** (data quality, FKs miss): Daily digest email with summary
- **Info errors** (out-of-range values): Log only, no email
- Include task name, error count, affected records, suggested remediation

#### DAG Definition Example

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['data-warehouse@company.com'],
    'email_on_failure': True,
    'start_date': datetime(2025, 1, 1)
}

dag = DAG(
    'dwh_etl_pipeline',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Daily at 1 AM
    description='Incremental DWH Load from PostgreSQL to ClickHouse with Error Handling'
)

# Tasks
extract_task = PythonOperator(
    task_id='extract_incremental_data',
    python_callable=extract_data_from_postgres,
    op_kwargs={'processing_date': '{{ ds }}'},
    provide_context=True
)

validate_task = PythonOperator(
    task_id='validate_extracted_data',
    python_callable=validate_data_quality,
    op_kwargs={'processing_date': '{{ ds }}'},
    provide_context=True
)

load_dim_customer_task = PythonOperator(
    task_id='load_dim_customer_scd2',
    python_callable=load_dimension_scd2,
    op_kwargs={'dimension': 'customer', 'processing_date': '{{ ds }}'},
    provide_context=True
)

load_dim_product_task = PythonOperator(
    task_id='load_dim_product_scd2',
    python_callable=load_dimension_scd2,
    op_kwargs={'dimension': 'product', 'processing_date': '{{ ds }}'},
    provide_context=True
)

load_fact_sales_task = PythonOperator(
    task_id='load_fact_sales',
    python_callable=load_fact_tables,
    op_kwargs={'fact_table': 'FactSales', 'processing_date': '{{ ds }}'},
    provide_context=True
)

update_aggregates_task = PythonOperator(
    task_id='update_aggregates',
    python_callable=update_aggregates,
    op_kwargs={'processing_date': '{{ ds }}'},
    provide_context=True
)

reprocess_errors_task = PythonOperator(
    task_id='reprocess_recoverable_errors',
    python_callable=reprocess_errors,
    op_kwargs={'processing_date': '{{ ds }}'},
    provide_context=True
)

# Dependencies
extract_task >> validate_task
validate_task >> [load_dim_customer_task, load_dim_product_task]
[load_dim_customer_task, load_dim_product_task] >> load_fact_sales_task
load_fact_sales_task >> update_aggregates_task
update_aggregates_task >> reprocess_errors_task
```

**Helper Function Example (Error Logging):**
```python
def log_error_record(clickhouse_client, error_type, error_message, 
                    failed_data, source_table, natural_key, 
                    processing_batch_id, task_name, is_recoverable):
    """Log error record to ClickHouse error_records table"""
    error_record = {
        'ErrorDate': datetime.now(),
        'SourceTable': source_table,
        'RecordNaturalKey': natural_key,
        'ErrorType': error_type,
        'ErrorSeverity': 'Critical' if not is_recoverable else 'Warning',
        'ErrorMessage': error_message,
        'FailedData': json.dumps(failed_data),
        'ProcessingBatchID': processing_batch_id,
        'TaskName': task_name,
        'IsRecoverable': int(is_recoverable),
        'RetryCount': 0,
        'LastAttemptDate': datetime.now(),
        'IsResolved': 0
    }
    
    try:
        clickhouse_client.insert('error_records', [error_record])
        logger.info(f"Logged error: {error_type} for {natural_key}")
    except Exception as e:
        logger.error(f"Failed to log error record: {str(e)}")
```

#### Logging & Monitoring

- Log all row counts: "Extracted 1,500 customers, loaded 50 new, updated 20, 3 errors"
- Log processing time: "Dimension load completed in 2.5 minutes"
- Track SCD Type 2 versions: "Created 45 new versions for customers"
- Monitor ClickHouse table sizes: Query `system.parts` table
- Alert on data freshness: If aggregate not updated within 2 hours of expected load time
- **Error Monitoring Dashboard Queries:**
  ```sql
  -- Daily error summary
  SELECT ErrorType, COUNT(*) as ErrorCount, SUM(IsRecoverable) as Recoverable
  FROM error_records
  WHERE ErrorDate >= today() - INTERVAL 7 DAY
  GROUP BY ErrorType
  ORDER BY ErrorCount DESC;
  
  -- Unresolved critical errors
  SELECT * FROM error_records
  WHERE IsResolved = 0 AND ErrorSeverity = 'Critical'
  ORDER BY ErrorDate DESC;
  ```

---

### Step 5: Change Data Capture (CDC) Strategy

#### For SCD Type 2 Dimensions

**Change Detection Method:**
1. Add `LAST_MODIFIED_DATE` column to source PostgreSQL tables
2. Query PostgreSQL: `WHERE LAST_MODIFIED_DATE > last_run_time`
3. In Airflow, store `last_run_time` in metadata table or Airflow variable

**Pseudocode for Change Detection:**
```python
def detect_changes(source_table, current_version):
    """
    Compare extracted data with current DimTable version
    Return: (inserts, updates)
    """
    inserts = extracted_data[~extracted_data['id'].isin(current_version['id'])]
    
    # Detect attribute changes
    common = extracted_data[extracted_data['id'].isin(current_version['id'])]
    merged = common.merge(current_version, on='id', suffixes=('_new', '_old'))
    
    # Check if any non-key columns changed
    attribute_cols = [col for col in common.columns if col != 'id']
    changes = merged[merged[attribute_cols].apply(
        lambda row: any(row[[f'{c}_new' for c in attribute_cols]] != 
                       row[[f'{c}_old' for c in attribute_cols]]), axis=1
    )]
    
    updates = merged[merged['id'].isin(changes['id'])]
    
    return inserts, updates
```

#### For Fact Tables (Append-Only)

- Fact tables are typically append-only (no updates)
- Exception: FactInventory (snapshot table) → DELETE old date snapshot, INSERT new
- If corrections needed: Load to correction fact table, then apply as adjustment

---

## Deliverables

### 1. SQL Scripts for ClickHouse Table Creation
- `01_create_dim_tables.sql` - All dimension table definitions (SCD Type 1 & Type 2)
- `02_create_fact_tables.sql` - All fact table definitions with appropriate engine
- `03_create_aggregate_tables.sql` - Aggregate table and materialized view definitions
- `04_create_error_tables.sql` - Error records and monitoring tables
- `05_create_indexes_and_partitioning.sql` - Optimization scripts

### 2. Apache Airflow DAG Python Scripts
- `dwh_etl_main_dag.py` - Main DAG orchestrating all tasks
- `extraction.py` - PostgreSQL data extraction functions with error handling
- `validation.py` - Data quality validation functions
- `transformation.py` - Data transformation and SCD logic
- `loading.py` - ClickHouse load functions with error logging
- `error_handling.py` - Error recording, classification, and reprocessing logic
- `utilities.py` - Helper functions (logging, error handling, metadata mgmt, alerting)

### 3. Documentation
- **Star Schema Design Document** (2-3 pages):
  - Entity-Relationship Diagram (ERD)
  - Fact and dimension definitions with all attributes
  - SCD strategy per dimension
  - Grain description for each fact table
  
- **ETL Pipeline Documentation** (3-4 pages):
  - Data flow diagram
  - Task dependencies and scheduling
  - Error handling and recovery procedures
  - Change detection methodology
  - Performance optimization notes
  - Error records table structure and usage

- **Error Handling & Monitoring Guide** (2-3 pages):
  - Error classification framework
  - Recoverable vs. non-recoverable error definitions
  - Error reprocessing procedures
  - Dashboard queries for error monitoring
  - Escalation procedures for unresolved errors

- **Operational Runbook** (2-3 pages):
  - How to monitor pipeline health
  - How to check and resolve errors from error_records table
  - Common troubleshooting steps
  - How to manually backfill data
  - Contacts and escalation procedures
  - Error investigation and resolution workflows

### 4. Presentation/Demo
- 15-20 minute presentation covering:
  - Data warehouse design rationale
  - Error handling architecture and strategy
  - Live demo: Extract one day of data from PostgreSQL
  - Live demo: Execute Airflow DAG for that day
  - Show ClickHouse tables populated with data
  - **Show error_records table with sample errors and resolution status**
  - Run sample analytical queries against aggregates
  - Performance metrics: row counts, load times, query performance, error rates
  - Lessons learned and next steps

---

## Evaluation Criteria

1. **Completeness and Correctness of Star Schema Design** (20%)
   - All dimension and fact tables properly defined
   - Appropriate SCD strategies applied with correct attributes
   - Accurate grain specifications
   - Proper relationships between facts and dimensions

2. **ClickHouse Table Implementation** (15%)
   - Correct engine selection (MergeTree, ReplacingMergeTree, SummingMergeTree)
   - Appropriate partitioning strategy
   - Correct ORDER BY and PRIMARY KEY definitions
   - Data type appropriateness (no unnecessary NULLs)
   - Error records table properly implemented

3. **Airflow ETL Pipeline Quality** (25%)
   - Proper DAG structure with correct task dependencies
   - Robust error handling and retry logic
   - Comprehensive logging and monitoring
   - Efficient incremental loading (day-by-day)
   - Correct SCD Type 2 merge logic

4. **Error Handling Implementation** (20%)
   - Error records table captures all relevant information
   - Error classification (recoverable vs. non-recoverable) correctly applied
   - Error reprocessing logic works correctly
   - Alerts and notifications configured properly
   - Error dashboard queries functional and informative

5. **Data Quality & Correctness** (10%)
   - Successful daily incremental loads
   - Accurate SCD Type 2 version tracking
   - Correct fact table grain maintenance
   - Accurate aggregate calculations
   - Proper error recording and resolution

6. **Documentation & Presentation** (10%)
   - Clear, professional documentation
   - Effective presentation of design and implementation
   - Detailed error handling explanation
   - Insightful demo showing end-to-end pipeline including error scenarios
   - Answers to technical questions

---

## Additional Notes & Best Practices

### Performance Optimization
- Use `PREWHERE` clause in ClickHouse for filtering on low-cardinality columns
- Consider `CODEC` compression for large text columns
- Monitor ClickHouse `system.parts` table to understand data layout
- Run `OPTIMIZE TABLE` after major data loads for deduplication

### Scalability Considerations
- For 100M+ row fact tables, partition by month or quarter
- For frequently-updated SCD Type 2 dimensions, use `ReplacingMergeTree` with deduplication
- Consider sharding strategy if data grows beyond single-server capacity
- Archive old error records (>90 days resolved) to separate table

### Data Validation
- Implement data quality checks in transformation layer
- Validate foreign key relationships before loading facts
- Use reconciliation queries: Row counts, SUM checks between PostgreSQL and ClickHouse
- Compare error rates week-over-week to detect data quality trends

### Future Enhancements
- Implement Change Data Capture (CDC) tools (e.g., Debezium) for real-time updates
- Add data quality framework (Great Expectations) with configurable rules
- Implement data lineage tracking for regulatory compliance
- Build Tableau/Superset dashboards on top of aggregate tables
- Machine learning models for anomaly detection in error patterns
- Automated error remediation for certain error types (e.g., FK misses)
