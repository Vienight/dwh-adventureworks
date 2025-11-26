-- Optimization and Maintenance Scripts

-- Add projections or secondary data skipping indexes
ALTER TABLE FactSales
ADD PROJECTION IF NOT EXISTS proj_sales_by_customer
(
    SELECT SalesDateKey, CustomerKey, sum(SalesAmount) AS Revenue
    GROUP BY SalesDateKey, CustomerKey
);

ALTER TABLE FactPurchases
ADD PROJECTION IF NOT EXISTS proj_purchases_by_vendor
(
    SELECT PurchaseDateKey, VendorKey, sum(PurchaseAmount) AS Amount
    GROUP BY PurchaseDateKey, VendorKey
);

-- TTL rules for error records (archive after 90 days)
ALTER TABLE error_records
MODIFY TTL ErrorDate + INTERVAL 90 DAY
DELETE WHERE IsResolved = 1;

-- Helper queries for system monitoring
SELECT
    table,
    sum(rows) AS total_rows,
    sum(bytes) AS total_bytes
FROM system.parts
WHERE database = currentDatabase()
GROUP BY table
ORDER BY total_bytes DESC;

-- Optimize statements to deduplicate ReplacingMergeTree tables
OPTIMIZE TABLE DimCustomer FINAL;
OPTIMIZE TABLE DimProduct FINAL;
OPTIMIZE TABLE DimStore FINAL;
OPTIMIZE TABLE DimEmployee FINAL;
OPTIMIZE TABLE DimVendor FINAL;
OPTIMIZE TABLE DimWarehouse FINAL;
OPTIMIZE TABLE DimSalesTerritory FINAL;

