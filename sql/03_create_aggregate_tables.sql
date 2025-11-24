-- ===================================================================
-- 03_create_aggregate_tables.sql
-- AdventureWorks DWH – All Aggregated Fact Tables (SummingMergeTree)
-- ===================================================================

-- 1. agg_daily_sales
-- Grain: Store + ProductCategory + Day
CREATE TABLE agg_daily_sales
(
    SalesDateKey          UInt32,
    StoreKey              UInt32,
    ProductCategoryKey    UInt32,
    
    TotalRevenue          Decimal(18, 2),
    TotalQuantity         UInt64,
    TotalDiscount         Decimal(18, 2),
    TransactionCount      UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (SalesDateKey, StoreKey, ProductCategoryKey)
PARTITION BY toYYYYMM(SalesDateKey);


-- 2. agg_weekly_sales
-- Grain: Week (WeekStartDateKey) + Region + ProductCategory
CREATE TABLE agg_weekly_sales
(
    WeekStartDateKey      UInt32,
    RegionKey             UInt32,
    ProductCategoryKey    UInt32,
    
    TotalRevenue          Decimal(18, 2),
    AvgRevenue            Decimal(18, 2),
    MinRevenue            Decimal(18, 2),
    MaxRevenue            Decimal(18, 2),
    TotalQuantity         UInt64,
    TransactionCount      UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (WeekStartDateKey, RegionKey, ProductCategoryKey)
PARTITION BY toYYYYMM(WeekStartDateKey);


-- 3. agg_monthly_sales
-- Grain: Month + CustomerSegment + Region
CREATE TABLE agg_monthly_sales
(
    MonthKey              UInt32,
    CustomerSegmentKey    UInt32,
    RegionKey             UInt32,
    
    TotalRevenue          Decimal(18, 2),
    NetRevenue            Decimal(18, 2),
    AvgOrderValue         Decimal(18, 2),
    CustomerCount         UInt64,
    OrderCount            UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (MonthKey, CustomerSegmentKey, RegionKey)
PARTITION BY toYYYYMM(MonthKey);


-- 4. agg_daily_inventory (NEW)
-- Grain: Day + Warehouse + ProductCategory + AgingTier
CREATE TABLE agg_daily_inventory
(
    InventoryDateKey      UInt32,
    WarehouseKey          UInt32,
    ProductCategoryKey    UInt32,
    AgingTierKey          UInt32,
    
    InventoryValue        Decimal(18, 2),   -- SUM(QuantityOnHand * ListPrice)
    QuantityOnHand        Int64,
    AvgStockAgingDays     Decimal(10, 2)
)
ENGINE = SummingMergeTree()
ORDER BY (InventoryDateKey, WarehouseKey, ProductCategoryKey, AgingTierKey)
PARTITION BY toYYYYMM(InventoryDateKey);


-- 5. agg_monthly_product_performance (NEW)
-- Grain: Month + Product + Store
CREATE TABLE agg_monthly_product_performance
(
    MonthKey              UInt32,
    ProductKey            UInt32,
    StoreKey              UInt32,
    
    TotalRevenue          Decimal(18, 2),
    UnitsSold             UInt64,
    UnitsReturned         UInt64,
    ReturnRate            Decimal(8, 4),    -- UnitsReturned / UnitsSold
    AvgCustomerRating     Decimal(3, 2),
    TransactionCount      UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (MonthKey, ProductKey, StoreKey)
PARTITION BY toYYYYMM(MonthKey);


-- 6. agg_regional_sales (NEW)
-- Grain: Month + Region + SalesTerritory
-- Includes growth calculations (will be populated via INSERT with logic)
CREATE TABLE agg_regional_sales
(
    MonthKey              UInt32,
    RegionKey             UInt32,
    TerritoryKey          UInt32,
    
    TotalRevenue          Decimal(18, 2),
    PreviousMonthRevenue  Decimal(18, 2),
    RevenueGrowthPct      Decimal(8, 4),    -- (Current - Prev) / Prev * 100
    TotalQuantity         UInt64,
    TransactionCount      UInt64,
    UniqueCustomers       UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (MonthKey, RegionKey, TerritoryKey)
PARTITION BY toYYYYMM(MonthKey);

-- Optional: Materialized Views for auto-population (example for agg_daily_sales)
-- Can create these later if want fully automatic updates
/*
CREATE MATERIALIZED VIEW mv_agg_daily_sales
TO agg_daily_sales
AS SELECT
    SalesDateKey,
    StoreKey,
    p.ProductCategoryKey,
    sum(SalesAmount)          AS TotalRevenue,
    sum(Quantity)             AS TotalQuantity,
    sum(DiscountAmount)       AS TotalDiscount,
    count()                   AS TransactionCount
FROM FactSales fs
JOIN DimProduct p ON fs.ProductKey = p.ProductKey
WHERE p.IsCurrent = 1
GROUP BY SalesDateKey, StoreKey, p.ProductCategoryKey;
*/
