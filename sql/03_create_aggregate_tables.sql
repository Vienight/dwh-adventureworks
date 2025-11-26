-- Aggregated Fact Tables and Materialized Views

CREATE TABLE IF NOT EXISTS agg_daily_sales
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
PARTITION BY toYYYYMM(toDate(SalesDateKey));

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_daily_sales
TO agg_daily_sales
AS
SELECT
    SalesDateKey,
    StoreKey,
    ProductCategoryKey,
    sum(SalesAmount) AS TotalRevenue,
    sum(Quantity) AS TotalQuantity,
    sum(DiscountAmount) AS TotalDiscount,
    count() AS TransactionCount
FROM FactSales
INNER JOIN DimProduct ON FactSales.ProductKey = DimProduct.ProductKey
INNER JOIN DimProductCategory ON DimProduct.Category = DimProductCategory.CategoryName
GROUP BY SalesDateKey, StoreKey, ProductCategoryKey;

CREATE TABLE IF NOT EXISTS agg_weekly_sales
(
    WeekStartDateKey UInt32,
    RegionKey UInt32,
    ProductCategoryKey UInt32,
    RevenueSum Decimal(18, 2),
    RevenueAvg Decimal(18, 2),
    RevenueMin Decimal(18, 2),
    RevenueMax Decimal(18, 2)
)
ENGINE = SummingMergeTree((RevenueSum, RevenueAvg, RevenueMin, RevenueMax))
ORDER BY (WeekStartDateKey, RegionKey, ProductCategoryKey)
PARTITION BY toYYYYMM(toDate(WeekStartDateKey));

CREATE TABLE IF NOT EXISTS agg_monthly_sales
(
    MonthStartDateKey UInt32,
    CustomerSegmentKey UInt32,
    RegionKey UInt32,
    TotalRevenue Decimal(18, 2),
    AvgOrderValue Decimal(18, 2),
    DistinctCustomerCount UInt64
)
ENGINE = SummingMergeTree((TotalRevenue, AvgOrderValue, DistinctCustomerCount))
ORDER BY (MonthStartDateKey, CustomerSegmentKey, RegionKey)
PARTITION BY toYYYYMM(toDate(MonthStartDateKey));

CREATE TABLE IF NOT EXISTS agg_daily_inventory
(
    InventoryDateKey UInt32,
    WarehouseKey UInt32,
    ProductCategoryKey UInt32,
    AgingTierKey UInt32,
    AverageInventoryValue Decimal(18, 2)
)
ENGINE = SummingMergeTree(AverageInventoryValue)
ORDER BY (InventoryDateKey, WarehouseKey, ProductCategoryKey, AgingTierKey)
PARTITION BY toYYYYMM(toDate(InventoryDateKey));

CREATE TABLE IF NOT EXISTS agg_monthly_product_performance
(
    MonthStartDateKey UInt32,
    ProductKey UInt32,
    StoreKey UInt32,
    Revenue Decimal(18, 2),
    UnitsSold UInt32,
    ReturnsRate Decimal(5, 2),
    AverageRating Decimal(5, 2)
)
ENGINE = SummingMergeTree((Revenue, UnitsSold, ReturnsRate, AverageRating))
ORDER BY (MonthStartDateKey, ProductKey, StoreKey)
PARTITION BY toYYYYMM(toDate(MonthStartDateKey));

CREATE TABLE IF NOT EXISTS agg_regional_sales
(
    MonthStartDateKey UInt32,
    RegionKey UInt32,
    SalesTerritoryKey UInt32,
    Revenue Decimal(18, 2),
    GrowthRate Decimal(6, 2)
)
ENGINE = SummingMergeTree((Revenue, GrowthRate))
ORDER BY (MonthStartDateKey, RegionKey, SalesTerritoryKey)
PARTITION BY toYYYYMM(toDate(MonthStartDateKey));


