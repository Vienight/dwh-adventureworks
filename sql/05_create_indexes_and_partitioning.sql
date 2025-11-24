
-- ===================================================================
-- 05_optimizations.sql
-- Advanced ClickHouse optimizations: Projections, TTL, Materialized Views
-- ===================================================================

-- 1. PROJECTIONS – faster analytical queries (top N, filtering)

-- Top 10 products by revenue per day (very common executive query)
ALTER TABLE FactSales ADD PROJECTION proj_top_products_daily
(
    SELECT 
        SalesDateKey,
        ProductKey,
        sum(SalesAmount) AS TotalRevenue,
        sum(Quantity) AS TotalQty
    GROUP BY SalesDateKey, ProductKey
    ORDER BY TotalRevenue DESC
);

-- Sales by region + customer segment (very frequent)
ALTER TABLE FactSales ADD PROJECTION proj_sales_by_region_segment
(
    SELECT 
        SalesDateKey,
        s.RegionKey,
        c.CustomerSegmentKey,
        sum(NetRevenue) AS NetRevenue,
        count() AS Transactions
    FROM FactSales fs
    JOIN DimStore s ON fs.StoreKey = s.StoreKey AND s.IsCurrent = 1
    JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey AND c.IsCurrent = 1
    GROUP BY SalesDateKey, s.RegionKey, c.CustomerSegmentKey
);

-- Inventory aging analysis (critical for supply chain)
ALTER TABLE FactInventory ADD PROJECTION proj_inventory_aging
(
    SELECT 
        InventoryDateKey,
        ProductKey,
        WarehouseKey,
        sum(QuantityOnHand) AS Qty,
        avg(StockAgingDays) AS AvgAging
    GROUP BY InventoryDateKey, ProductKey, WarehouseKey
    ORDER BY AvgAging DESC
);


-- 2. TTL – automatic archiving/deletion of old data

-- Keep detailed FactSales only for last 3 years → older goes to cold storage
ALTER TABLE FactSales 
MODIFY TTL SalesDateKey + INTERVAL 3 YEAR DELETE;

-- Keep error records only 90 days if resolved, 2 years if unresolved
ALTER TABLE error_records 
MODIFY TTL ErrorDate + INTERVAL 90 DAY DELETE 
WHERE IsResolved = 1;

ALTER TABLE error_records 
MODIFY TTL ErrorDate + INTERVAL 730 DAY DELETE 
WHERE IsResolved = 0;


-- 3. MATERIALIZED VIEWS – automatic unit maintenance (super bonus!)

-- agg_daily_sales – auto-populated every time new data arrives
CREATE MATERIALIZED VIEW mv_agg_daily_sales
TO agg_daily_sales
AS
SELECT
    fs.SalesDateKey,
    fs.StoreKey,
    p.ProductCategoryKey,
    sum(fs.SalesAmount)          AS TotalRevenue,
    sum(fs.Quantity)             AS TotalQuantity,
    sum(fs.DiscountAmount)       AS TotalDiscount,
    count()                      AS TransactionCount
FROM FactSales fs
JOIN DimProduct p ON fs.ProductKey = p.ProductKey AND p.IsCurrent = 1
GROUP BY
    fs.SalesDateKey,
    fs.StoreKey,
    p.ProductCategoryKey;

-- agg_monthly_sales – auto-populated
CREATE MATERIALIZED VIEW mv_agg_monthly_sales
TO agg_monthly_sales
AS
SELECT
    toStartOfMonth(toDate(fs.SalesDateKey)) AS MonthKey,
    c.CustomerSegmentKey,
    s.RegionKey,
    sum(fs.NetRevenue)               AS NetRevenue,
    sum(fs.SalesAmount)              AS TotalRevenue,
    avg(fs.SalesAmount)              AS AvgOrderValue,
    count(DISTINCT fs.CustomerKey)   AS CustomerCount,
    count()                          AS OrderCount
FROM FactSales fs
JOIN DimCustomer c ON fs.CustomerKey = c.CustomerKey AND c.IsCurrent = 1
JOIN DimStore s ON fs.StoreKey = s.StoreKey AND s.IsCurrent = 1
GROUP BY
    MonthKey,
    c.CustomerSegmentKey,
    s.RegionKey;


-- 4. FINAL TOUCH – set recommended settings for large tables
ALTER TABLE FactSales      MODIFY SETTING index_granularity = 8192, merge_with_ttl_timeout = 86400;
ALTER TABLE FactInventory  MODIFY SETTING index_granularity = 8192;
ALTER TABLE agg_daily_sales MODIFY SETTING index_granularity = 4096;

-- Optional: force merge old parts (run manually once a month)
-- OPTIMIZE TABLE FactSales FINAL;
-- OPTIMIZE TABLE error_records FINAL;
