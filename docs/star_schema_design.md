# Star Schema Design

## Source Overview
- AdventureWorks OLTP data loaded from `data/postgresDBSamples/postgresDBSamples-master/adventureworks`.
- PostgreSQL serves as landing zone; ClickHouse hosts the dimensional Warehouse.

## Dimension Tables
- **DimDate (SCD1):** rich calendar attributes, partitioned by `FullDate`.
- **DimCustomer (SCD2):** history tracked on contact/segment/status attributes; `ReplacingMergeTree`.
- **DimProduct (SCD2):** history for price/cost/category/status.
- **DimStore (SCD2):** tracks geo/manager/status changes.
- **DimEmployee (SCD2):** captures role/region/quota updates with self-reference for managers.
- **DimPromotion (configurable SCD1/2):** metadata about campaigns and targets.
- **DimVendor (SCD2):** monitors supplier KPIs and statuses.
- **Static SCD1 dimensions:** `DimFeedbackCategory`, `DimReturnReason`, `DimCustomerSegment`, `DimAgingTier`, `DimFinanceCategory`, `DimRegion`, `DimProductCategory`.
- **Optional DimWarehouse & DimSalesTerritory (SCD2)** for logistics coverage.

## Fact Tables
- **FactSales:** grain = one row per sales order line; metrics revenue, quantity, discount, transaction count.
- **FactPurchases:** purchase order line grain; tracks supplier spend.
- **FactInventory:** daily snapshot factless table for product availability by store/warehouse.
- **FactProduction:** production batch performance metrics.
- **FactEmployeeSales:** per-employee per-day performance vs target.
- **FactCustomerFeedback:** per feedback submission measurements.
- **FactPromotionResponse:** product/store/promotion/day results to analyze campaign lift.
- **FactFinance:** invoice-level financial metrics.
- **FactReturns:** return line metrics tied to reasons.

## Aggregated Facts
- **agg_daily_sales:** store + product category daily performance (materialized view).
- **agg_weekly_sales:** region + category weekly summary (min/max/avg).
- **agg_monthly_sales:** monthly revenue by customer segment and region.
- **agg_daily_inventory:** warehouse/category/aging tier averages.
- **agg_monthly_product_performance:** monthly KPIs by product/store.
- **agg_regional_sales:** regional revenue plus growth rates.

## Relationships & Grain
- All fact tables reference surrogate keys from corresponding dimensions (`CustomerKey`, `ProductKey`, etc.) plus date surrogate `DateKey`.
- Date surrogate derived from `YYYYMMDD`.
- Inventory fact uses daily snapshot grain; production uses batch grain; aggregated tables summarized as specified.

## Partitioning & Ordering
- MergeTree tables partitioned monthly via date surrogate for pruning.
- `ORDER BY` uses dimension keys for locality and query speed.
- Aggregated tables use `SummingMergeTree` to optimize rollups; SCD2 dims use `ReplacingMergeTree`.

## Change Data Capture Strategy
- Source tables rely on `ModifiedDate` for incremental extraction.
- Warehouse uses `ValidFromDate`, `ValidToDate`, `IsCurrent` to maintain slowly changing history.
- Aggregations refresh incrementally for the processing date (daily/weekly/monthly).

