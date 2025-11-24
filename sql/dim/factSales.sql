CREATE TABLE fact_sales
(
    SalesDateKey UInt32,
    CustomerKey UInt32,
    ProductKey UInt32,
    StoreKey UInt32,
    SalesAmount Decimal(18,2),
    Quantity UInt32,
    DiscountAmount Decimal(18,2)
)
ENGINE = MergeTree()
ORDER BY (SalesDateKey, CustomerKey, ProductKey, StoreKey);
