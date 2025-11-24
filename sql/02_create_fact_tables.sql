
-- ===================================================================
-- 02_create_fact_tables.sql
-- AdventureWorks DWH – All Fact Tables (Core + Extended)
-- ===================================================================

-- FactSales (main transactional fact)
CREATE TABLE FactSales
(
    SalesDateKey        UInt32,
    CustomerKey         UInt32,
    ProductKey          UInt32,
    StoreKey            UInt32,
    EmployeeKey         UInt32,
    PromotionKey        UInt32,
    SalesAmount         Decimal(18, 2),
    Quantity            UInt32,
    DiscountAmount      Decimal(18, 2),
    TransactionCount    UInt32,
    UnitPrice           Decimal(18, 2),
    NetRevenue          Decimal(18, 2),
    GrossProfit         Decimal(18, 2)
)
ENGINE = MergeTree()
ORDER BY (SalesDateKey, CustomerKey, ProductKey, StoreKey)
PARTITION BY toYYYYMM(SalesDateKey);

-- FactPurchases
CREATE TABLE FactPurchases
(
    PurchaseDateKey     UInt32,
    ProductKey          UInt32,
    VendorKey           UInt32,
    PurchaseAmount      Decimal(18, 2),
    PurchaseQuantity    UInt32,
    UnitCost            Decimal(18, 2),
    DiscountAmount      Decimal(18, 2)
)
ENGINE = MergeTree()
ORDER BY (PurchaseDateKey, ProductKey, VendorKey)
PARTITION BY toYYYYMM(PurchaseDateKey);

-- FactInventory (daily snapshot – factless + measures)
CREATE TABLE FactInventory
(
    InventoryDateKey    UInt32,
    ProductKey          UInt32,
    StoreKey            UInt32,
    WarehouseKey        UInt32,
    QuantityOnHand      Int32,
    ReorderLevel        UInt32,
    SafetyStock         UInt32,
    StockAgingDays      UInt32
)
ENGINE = MergeTree()
ORDER BY (InventoryDateKey, ProductKey, StoreKey)
PARTITION BY toYYYYMM(InventoryDateKey);

-- FactProduction
CREATE TABLE FactProduction
(
    ProductionDateKey   UInt32,
    ProductKey          UInt32,
    EmployeeKey         UInt32,
    UnitsProduced       UInt32,
    ProductionTimeHours Decimal(10, 2),
    ScrapRate           Decimal(5, 4),
    DefectCount         UInt32
)
ENGINE = MergeTree()
ORDER BY (ProductionDateKey, ProductKey)
PARTITION BY toYYYYMM(ProductionDateKey);

-- FactEmployeeSales (daily employee performance)
CREATE TABLE FactEmployeeSales
(
    SalesDateKey        UInt32,
    EmployeeKey         UInt32,
    StoreKey            UInt32,
    TerritoryKey        UInt32,
    SalesAmount         Decimal(18, 2),
    Quantity            UInt32,
    TransactionCount    UInt32
)
ENGINE = MergeTree()
ORDER BY (SalesDateKey, EmployeeKey)
PARTITION BY toYYYYMM(SalesDateKey);

-- FactCustomerFeedback
CREATE TABLE FactCustomerFeedback
(
    FeedbackDateKey     UInt32,
    CustomerKey         UInt32,
    EmployeeKey         UInt32,
    FeedbackCategoryKey UInt32,
    Score               UInt8,                    -- 1-5
    ResolutionTimeHours Decimal(8, 2),
    CSAT                Decimal(4, 2)
)
ENGINE = MergeTree()
ORDER BY (FeedbackDateKey, CustomerKey)
PARTITION BY toYYYYMM(FeedbackDateKey);

-- FactPromotionResponse
CREATE TABLE FactPromotionResponse
(
    PromotionDateKey    UInt32,
    PromotionKey        UInt32,
    ProductKey          UInt32,
    StoreKey            UInt32,
    SalesDuringPromo    Decimal(18, 2),
    DiscountUsedCount   UInt32,
    UptakeRate          Decimal(6, 4)
)
ENGINE = MergeTree()
ORDER BY (PromotionDateKey, PromotionKey, ProductKey)
PARTITION BY toYYYYMM(PromotionDateKey);

-- FactFinance
CREATE TABLE FactFinance
(
    InvoiceDateKey      UInt32,
    CustomerKey         UInt32,
    StoreKey            UInt32,
    FinanceCategoryKey  UInt32,
    InvoiceAmount       Decimal(18, 2),
    PaymentDelayDays    Int16,
    CreditUsage         Decimal(18, 2)
)
ENGINE = MergeTree()
ORDER BY (InvoiceDateKey, CustomerKey)
PARTITION BY toYYYYMM(InvoiceDateKey);

-- FactReturns
CREATE TABLE FactReturns
(
    ReturnDateKey       UInt32,
    ProductKey          UInt32,
    CustomerKey         UInt32,
    StoreKey            UInt32,
    ReturnReasonKey     UInt32,
    ReturnedQuantity    UInt32,
    RefundAmount        Decimal(18, 2),
    RestockingFee       Decimal(18, 2)
)
ENGINE = MergeTree()
ORDER BY (ReturnDateKey, ProductKey, CustomerKey)
PARTITION BY toYYYYMM(ReturnDateKey);
