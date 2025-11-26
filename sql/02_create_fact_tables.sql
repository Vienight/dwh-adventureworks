-- ClickHouse Fact Tables for AdventureWorks DWH

CREATE TABLE IF NOT EXISTS FactSales
(
    SalesDateKey UInt32,
    CustomerKey UInt32,
    ProductKey UInt32,
    StoreKey UInt32,
    EmployeeKey UInt32,
    SalesAmount Decimal(18, 2),
    Quantity UInt32,
    DiscountAmount Decimal(18, 2),
    TransactionCount UInt32,
    OrderNumber String
)
ENGINE = MergeTree()
ORDER BY (SalesDateKey, StoreKey, ProductKey, CustomerKey)
PARTITION BY toYYYYMM(toDate(SalesDateKey));

CREATE TABLE IF NOT EXISTS FactPurchases
(
    PurchaseDateKey UInt32,
    ProductKey UInt32,
    VendorKey UInt32,
    PurchaseAmount Decimal(18, 2),
    PurchaseQuantity UInt32,
    DiscountAmount Decimal(18, 2),
    UnitCost Decimal(18, 2),
    PurchaseOrderNumber String
)
ENGINE = MergeTree()
ORDER BY (PurchaseDateKey, VendorKey, ProductKey)
PARTITION BY toYYYYMM(toDate(PurchaseDateKey));

CREATE TABLE IF NOT EXISTS FactInventory
(
    InventoryDateKey UInt32,
    ProductKey UInt32,
    StoreKey UInt32,
    WarehouseKey UInt32,
    QuantityOnHand Int32,
    StockAgingDays UInt16,
    ReorderLevel Int32,
    SafetyStock Int32
)
ENGINE = MergeTree()
ORDER BY (InventoryDateKey, WarehouseKey, ProductKey)
PARTITION BY toYYYYMM(toDate(InventoryDateKey));

CREATE TABLE IF NOT EXISTS FactProduction
(
    ProductionDateKey UInt32,
    ProductKey UInt32,
    EmployeeKey UInt32,
    UnitsProduced UInt32,
    ProductionTimeHours Float32,
    ScrapRate Decimal(5, 2),
    DefectCount UInt32,
    ProductionBatch String
)
ENGINE = MergeTree()
ORDER BY (ProductionDateKey, ProductKey, EmployeeKey)
PARTITION BY toYYYYMM(toDate(ProductionDateKey));

CREATE TABLE IF NOT EXISTS FactEmployeeSales
(
    SalesDateKey UInt32,
    EmployeeKey UInt32,
    StoreKey UInt32,
    SalesTerritoryKey UInt32,
    SalesAmount Decimal(18, 2),
    SalesTarget Decimal(18, 2),
    CustomerContacts UInt32
)
ENGINE = MergeTree()
ORDER BY (SalesDateKey, EmployeeKey, StoreKey)
PARTITION BY toYYYYMM(toDate(SalesDateKey));

CREATE TABLE IF NOT EXISTS FactCustomerFeedback
(
    FeedbackDateKey UInt32,
    CustomerKey UInt32,
    EmployeeKey UInt32,
    FeedbackCategoryKey UInt32,
    FeedbackScore UInt8,
    ComplaintCount UInt32,
    ResolutionTimeHours Float32,
    CSATScore Decimal(5, 2)
)
ENGINE = MergeTree()
ORDER BY (FeedbackDateKey, CustomerKey, FeedbackCategoryKey)
PARTITION BY toYYYYMM(toDate(FeedbackDateKey));

CREATE TABLE IF NOT EXISTS FactPromotionResponse
(
    PromotionDateKey UInt32,
    ProductKey UInt32,
    StoreKey UInt32,
    PromotionKey UInt32,
    SalesDuringCampaign Decimal(18, 2),
    DiscountUsageCount UInt32,
    CustomerUptakeRate Decimal(5, 2),
    PromotionROI Decimal(8, 2)
)
ENGINE = MergeTree()
ORDER BY (PromotionDateKey, PromotionKey, ProductKey, StoreKey)
PARTITION BY toYYYYMM(toDate(PromotionDateKey));

CREATE TABLE IF NOT EXISTS FactFinance
(
    InvoiceDateKey UInt32,
    FinanceCategoryKey UInt32,
    CustomerKey UInt32,
    StoreKey UInt32,
    InvoiceAmount Decimal(18, 2),
    PaymentDelayDays Int32,
    CreditUsage Decimal(5, 2),
    InterestCharges Decimal(18, 2),
    InvoiceNumber String
)
ENGINE = MergeTree()
ORDER BY (InvoiceDateKey, FinanceCategoryKey, CustomerKey)
PARTITION BY toYYYYMM(toDate(InvoiceDateKey));

CREATE TABLE IF NOT EXISTS FactReturns
(
    ReturnDateKey UInt32,
    ProductKey UInt32,
    CustomerKey UInt32,
    StoreKey UInt32,
    ReturnReasonKey UInt32,
    ReturnedQuantity UInt32,
    RefundAmount Decimal(18, 2),
    RestockingFee Decimal(18, 2),
    ReturnAuthorizationNumber String
)
ENGINE = MergeTree()
ORDER BY (ReturnDateKey, ProductKey, CustomerKey)
PARTITION BY toYYYYMM(toDate(ReturnDateKey));


