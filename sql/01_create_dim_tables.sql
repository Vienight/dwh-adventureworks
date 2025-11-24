-- ===================================================================
-- 01_create_dim_tables.sql
-- AdventureWorks DWH – All Dimension Tables for ClickHouse
-- ===================================================================

-- DimDate (SCD Type 1 – static reference table)
CREATE TABLE DimDate
(
    DateKey             UInt32,
    FullDate            Date,
    Year                UInt16,
    Quarter             UInt8,
    Month               UInt8,
    MonthName           LowCardinality(String),
    Week                UInt8,
    DayOfWeek           UInt8,
    DayName             LowCardinality(String),
    DayOfMonth          UInt8,
    DayOfYear           UInt16,
    WeekOfYear          UInt8,
    IsWeekend           UInt8,
    IsHoliday           UInt8,
    HolidayName         LowCardinality(String),
    FiscalYear          UInt16,
    FiscalQuarter       UInt8,
    FiscalMonth         UInt8,
    Season              LowCardinality(String)
)
ENGINE = MergeTree()
PRIMARY KEY (DateKey)
ORDER BY (DateKey);

-- DimCustomer (SCD Type 2)
CREATE TABLE DimCustomer
(
    CustomerKey             UInt32,
    CustomerID              UInt32,
    CustomerName            String,
    Email                   String,
    Phone                   String,
    City                    LowCardinality(String),
    StateProvince           LowCardinality(String),
    Country                 LowCardinality(String),
    PostalCode              String,
    CustomerSegment         LowCardinality(String),
    CustomerType            LowCardinality(String),
    AccountStatus           LowCardinality(String),
    CreditLimit             Decimal(18,2),
    AnnualIncome            Decimal(18,2),
    YearsSinceFirstPurchase UInt16,
    ValidFromDate           Date,
    ValidToDate             Nullable(Date),
    IsCurrent               UInt8,
    SourceUpdateDate        Date,
    EffectiveStartDate      Date,
    EffectiveEndDate        Nullable(Date)
)
ENGINE = ReplacingMergeTree(EffectiveEndDate)
PRIMARY KEY (CustomerKey)
ORDER BY (CustomerKey, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

-- DimProduct (SCD Type 2)
CREATE TABLE DimProduct
(
    ProductKey          UInt32,
    ProductID           UInt32,
    ProductName         String,
    SKU                 String,
    Category            LowCardinality(String),
    SubCategory         LowCardinality(String),
    Brand               LowCardinality(String),
    ListPrice           Decimal(18,2),
    Cost                Decimal(18,2),
    ProductStatus       LowCardinality(String),
    Color               LowCardinality(String),
    Size                LowCardinality(String),
    Weight              Decimal(10,3),
    ValidFromDate       Date,
    ValidToDate         Nullable(Date),
    IsCurrent           UInt8,
    SourceUpdateDate    Date,
    EffectiveStartDate  Date,
    EffectiveEndDate    Nullable(Date)
)
ENGINE = ReplacingMergeTree(EffectiveEndDate)
PRIMARY KEY (ProductKey)
ORDER BY (ProductKey, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

-- DimStore (SCD Type 2)
CREATE TABLE DimStore
(
    StoreKey            UInt32,
    StoreID             UInt32,
    StoreName           String,
    StoreNumber         UInt32,
    Address             String,
    City                LowCardinality(String),
    StateProvince       LowCardinality(String),
    Country             LowCardinality(String),
    PostalCode          String,
    Region              LowCardinality(String),
    Territory           LowCardinality(String),
    StoreType           LowCardinality(String),
    StoreStatus         LowCardinality(String),
    ManagerName         String,
    OpeningDate         Date,
    SquareFootage       UInt32,
    ValidFromDate       Date,
    ValidToDate         Nullable(Date),
    IsCurrent           UInt8,
    SourceUpdateDate    Date
)
ENGINE = ReplacingMergeTree(ValidToDate)
PRIMARY KEY (StoreKey)
ORDER BY (StoreKey, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

-- DimEmployee (SCD Type 2)
CREATE TABLE DimEmployee
(
    EmployeeKey             UInt32,
    EmployeeID              UInt32,
    EmployeeName            String,
    JobTitle                LowCardinality(String),
    Department              LowCardinality(String),
    ReportingManagerKey     UInt32,
    HireDate                Date,
    EmployeeStatus          LowCardinality(String),
    Region                  LowCardinality(String),
    Territory               LowCardinality(String),
    SalesQuota              Decimal(18,2),
    ValidFromDate           Date,
    ValidToDate             Nullable(Date),
    IsCurrent               UInt8,
    SourceUpdateDate        Date
)
ENGINE = ReplacingMergeTree(ValidToDate)
PRIMARY KEY (EmployeeKey)
ORDER BY (EmployeeKey, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

-- DimPromotion (SCD Type 1 – as reccomended in task)
CREATE TABLE DimPromotion
(
    PromotionKey            UInt32,
    PromotionID             UInt32,
    PromotionName           String,
    PromotionDescription    String,
    PromotionType           LowCardinality(String),
    DiscountPercentage      Decimal(5,2),
    DiscountAmount          Decimal(18,2),
    StartDate               Date,
    EndDate                 Nullable(Date),
    IsActive                UInt8,
    PromotionStatus         LowCardinality(String),
    CampaignID              UInt32,
    TargetProductKey        Nullable(UInt32),
    TargetCustomerSegment   LowCardinality(String)
)
ENGINE = MergeTree()
PRIMARY KEY (PromotionKey)
ORDER BY (PromotionKey);

-- DimVendor (SCD Type 2)
CREATE TABLE DimVendor
(
    VendorKey               UInt32,
    VendorID                UInt32,
    VendorName              String,
    ContactPerson           String,
    Email                   String,
    Phone                   String,
    Address                 String,
    City                    LowCardinality(String),
    Country                 LowCardinality(String),
    VendorRating            Decimal(3,2),
    OnTimeDeliveryRate      Decimal(5,2),
    QualityScore            Decimal(5,2),
    PaymentTerms            String,
    VendorStatus            LowCardinality(String),
    ValidFromDate           Date,
    ValidToDate             Nullable(Date),
    IsCurrent               UInt8,
    SourceUpdateDate        Date
)
ENGINE = ReplacingMergeTree(ValidToDate)
PRIMARY KEY (VendorKey)
ORDER BY (VendorKey, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

-- SCD Type 1 – static dimensions
CREATE TABLE DimFeedbackCategory
(
    FeedbackCategoryKey     UInt32,
    FeedbackCategoryID      UInt32,
    CategoryName            LowCardinality(String),
    CategoryDescription     String
)
ENGINE = MergeTree()
PRIMARY KEY (FeedbackCategoryKey)
ORDER BY (FeedbackCategoryKey);

CREATE TABLE DimReturnReason
(
    ReturnReasonKey         UInt32,
    ReturnReasonID          UInt32,
    ReturnReasonName        LowCardinality(String),
    ReturnReasonDescription String
)
ENGINE = MergeTree()
PRIMARY KEY (ReturnReasonKey)
ORDER BY (ReturnReasonKey);

CREATE TABLE DimWarehouse
(
    WarehouseKey            UInt32,
    WarehouseID             UInt32,
    WarehouseName           String,
    Location                String,
    WarehouseType           LowCardinality(String),
    ManagerKey              UInt32,
    ValidFromDate           Date,
    ValidToDate             Nullable(Date),
    IsCurrent               UInt8
)
ENGINE = ReplacingMergeTree(ValidToDate)
PRIMARY KEY (WarehouseKey)
ORDER BY (WarehouseKey, ValidFromDate);

CREATE TABLE DimSalesTerritory
(
    TerritoryKey            UInt32,
    TerritoryID             UInt32,
    TerritoryName           LowCardinality(String),
    SalesRegion             LowCardinality(String),
    Country                 LowCardinality(String),
    Manager                 String,
    SalesTarget             Decimal(18,2)
)
ENGINE = MergeTree()
PRIMARY KEY (TerritoryKey)
ORDER BY (TerritoryKey);

CREATE TABLE DimCustomerSegment
(
    SegmentKey              UInt32,
    SegmentID               UInt32,
    SegmentName             LowCardinality(String),
    SegmentDescription      String,
    DiscountTierStart       Decimal(5,2),
    DiscountTierEnd         Decimal(5,2)
)
ENGINE = MergeTree()
PRIMARY KEY (SegmentKey)
ORDER BY (SegmentKey);

CREATE TABLE DimAgingTier
(
    AgingTierKey            UInt32,
    AgingTierID             UInt32,
    AgingTierName           LowCardinality(String),
    MinAgingDays            UInt16,
    MaxAgingDays            UInt16
)
ENGINE = MergeTree()
PRIMARY KEY (AgingTierKey)
ORDER BY (AgingTierKey);

CREATE TABLE DimFinanceCategory
(
    FinanceCategoryKey      UInt32,
    FinanceCategoryID       UInt32,
    CategoryName            LowCardinality(String),
    CategoryDescription     String
)
ENGINE = MergeTree()
PRIMARY KEY (FinanceCategoryKey)
ORDER BY (FinanceCategoryKey);

CREATE TABLE DimRegion
(
    RegionKey               UInt32,
    RegionID                UInt32,
    RegionName              LowCardinality(String),
    Country                 LowCardinality(String),
    Continent               LowCardinality(String),
    TimeZone                String
)
ENGINE = MergeTree()
PRIMARY KEY (RegionKey)
ORDER BY (RegionKey);

CREATE TABLE DimProductCategory
(
    ProductCategoryKey      UInt32,
    ProductCategoryID       UInt32,
    CategoryName            LowCardinality(String),
    CategoryDescription     String
)
ENGINE = MergeTree()
PRIMARY KEY (ProductCategoryKey)
ORDER BY (ProductCategoryKey);

-- END OF FILE
