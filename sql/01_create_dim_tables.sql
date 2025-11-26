-- ClickHouse Dimension Tables
-- Generated for AdventureWorks-based DWH

CREATE TABLE IF NOT EXISTS DimDate
(
    DateKey UInt32,
    FullDate Date,
    Year UInt16,
    Quarter UInt8,
    Month UInt8,
    MonthName String,
    Week UInt8,
    DayOfWeek UInt8,
    DayName String,
    DayOfMonth UInt8,
    DayOfYear UInt16,
    WeekOfYear UInt8,
    IsWeekend UInt8,
    IsHoliday UInt8,
    HolidayName String,
    FiscalYear UInt16,
    FiscalQuarter UInt8,
    FiscalMonth UInt8,
    Season String
)
ENGINE = MergeTree()
ORDER BY (DateKey)
PARTITION BY toYYYYMM(FullDate);

CREATE TABLE IF NOT EXISTS DimCustomer
(
    CustomerKey UInt32,
    CustomerID UInt32,
    CustomerName String,
    Email String,
    Phone String,
    City String,
    StateProvince String,
    Country String,
    PostalCode String,
    CustomerSegment String,
    CustomerType String,
    AccountStatus String,
    CreditLimit Decimal(18, 2),
    AnnualIncome Decimal(18, 2),
    YearsSinceFirstPurchase UInt16,
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Nullable(Date)
)
ENGINE = ReplacingMergeTree(ValidFromDate)
ORDER BY (CustomerID, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

CREATE TABLE IF NOT EXISTS DimProduct
(
    ProductKey UInt32,
    ProductID UInt32,
    ProductName String,
    SKU String,
    Category String,
    SubCategory String,
    Brand String,
    ListPrice Decimal(18, 2),
    Cost Decimal(18, 2),
    ProductStatus String,
    Color String,
    Size String,
    Weight Decimal(10, 3),
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date,
    EffectiveStartDate Date,
    EffectiveEndDate Nullable(Date)
)
ENGINE = ReplacingMergeTree(ValidFromDate)
ORDER BY (ProductID, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

CREATE TABLE IF NOT EXISTS DimStore
(
    StoreKey UInt32,
    StoreID UInt32,
    StoreName String,
    StoreNumber UInt32,
    Address String,
    City String,
    StateProvince String,
    Country String,
    PostalCode String,
    Region String,
    Territory String,
    StoreType String,
    StoreStatus String,
    ManagerName String,
    OpeningDate Date,
    SquareFootage UInt32,
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date
)
ENGINE = ReplacingMergeTree(ValidFromDate)
ORDER BY (StoreID, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

CREATE TABLE IF NOT EXISTS DimEmployee
(
    EmployeeKey UInt32,
    EmployeeID UInt32,
    EmployeeName String,
    JobTitle String,
    Department String,
    ReportingManagerKey Nullable(UInt32),
    HireDate Date,
    EmployeeStatus String,
    Region String,
    Territory String,
    SalesQuota Decimal(18, 2),
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date
)
ENGINE = ReplacingMergeTree(ValidFromDate)
ORDER BY (EmployeeID, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

CREATE TABLE IF NOT EXISTS DimPromotion
(
    PromotionKey UInt32,
    PromotionID UInt32,
    PromotionName String,
    PromotionDescription String,
    PromotionType String,
    DiscountPercentage Decimal(5, 2),
    DiscountAmount Decimal(18, 2),
    StartDate Date,
    EndDate Nullable(Date),
    IsActive UInt8,
    PromotionStatus String,
    CampaignID UInt32,
    TargetProductKey Nullable(UInt32),
    TargetCustomerSegment Nullable(String)
)
ENGINE = MergeTree()
ORDER BY (PromotionKey)
PARTITION BY toYYYYMM(StartDate);

CREATE TABLE IF NOT EXISTS DimVendor
(
    VendorKey UInt32,
    VendorID UInt32,
    VendorName String,
    ContactPerson String,
    Email String,
    Phone String,
    Address String,
    City String,
    Country String,
    VendorRating Decimal(3, 2),
    OnTimeDeliveryRate Decimal(5, 2),
    QualityScore Decimal(5, 2),
    PaymentTerms String,
    VendorStatus String,
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8,
    SourceUpdateDate Date
)
ENGINE = ReplacingMergeTree(ValidFromDate)
ORDER BY (VendorID, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

CREATE TABLE IF NOT EXISTS DimFeedbackCategory
(
    FeedbackCategoryKey UInt32,
    FeedbackCategoryID UInt32,
    CategoryName String,
    CategoryDescription String
)
ENGINE = MergeTree()
ORDER BY (FeedbackCategoryKey);

CREATE TABLE IF NOT EXISTS DimReturnReason
(
    ReturnReasonKey UInt32,
    ReturnReasonID UInt32,
    ReturnReasonName String,
    ReturnReasonDescription String
)
ENGINE = MergeTree()
ORDER BY (ReturnReasonKey);

CREATE TABLE IF NOT EXISTS DimWarehouse
(
    WarehouseKey UInt32,
    WarehouseID UInt32,
    WarehouseName String,
    Location String,
    WarehouseType String,
    ManagerKey Nullable(UInt32),
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8
)
ENGINE = ReplacingMergeTree(ValidFromDate)
ORDER BY (WarehouseID, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

CREATE TABLE IF NOT EXISTS DimSalesTerritory
(
    TerritoryKey UInt32,
    TerritoryID UInt32,
    TerritoryName String,
    SalesRegion String,
    Country String,
    Manager String,
    SalesTarget Decimal(18, 2),
    ValidFromDate Date,
    ValidToDate Nullable(Date),
    IsCurrent UInt8
)
ENGINE = ReplacingMergeTree(ValidFromDate)
ORDER BY (TerritoryID, ValidFromDate)
PARTITION BY toYYYYMM(ValidFromDate);

CREATE TABLE IF NOT EXISTS DimCustomerSegment
(
    SegmentKey UInt32,
    SegmentID UInt32,
    SegmentName String,
    SegmentDescription String,
    DiscountTierStart Decimal(5, 2),
    DiscountTierEnd Decimal(5, 2)
)
ENGINE = MergeTree()
ORDER BY (SegmentKey);

CREATE TABLE IF NOT EXISTS DimAgingTier
(
    AgingTierKey UInt32,
    AgingTierID UInt32,
    AgingTierName String,
    MinAgingDays UInt16,
    MaxAgingDays UInt16
)
ENGINE = MergeTree()
ORDER BY (AgingTierKey);

CREATE TABLE IF NOT EXISTS DimFinanceCategory
(
    FinanceCategoryKey UInt32,
    FinanceCategoryID UInt32,
    CategoryName String,
    CategoryDescription String
)
ENGINE = MergeTree()
ORDER BY (FinanceCategoryKey);

CREATE TABLE IF NOT EXISTS DimRegion
(
    RegionKey UInt32,
    RegionID UInt32,
    RegionName String,
    Country String,
    Continent String,
    TimeZone String
)
ENGINE = MergeTree()
ORDER BY (RegionKey);

CREATE TABLE IF NOT EXISTS DimProductCategory
(
    ProductCategoryKey UInt32,
    ProductCategoryID UInt32,
    CategoryName String,
    CategoryDescription String
)
ENGINE = MergeTree()
ORDER BY (ProductCategoryKey);


