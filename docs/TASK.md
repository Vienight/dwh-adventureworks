# AdventureWorks Data Warehouse — Progress Tracker

## 📌 Intern Task Requirements

- [ ] Load AdventureWorks source DB into PostgreSQL
- [ ] Design DWH Star Schema (Facts + Dimensions)
- [ ] Create ClickHouse DWH tables
  - [ ] Dim Tables (DimDate, DimCustomer, ...)
  - [ ] Fact Tables (FactSales, ...)
  - [ ] Aggregated Tables (agg_daily_sales, ...)
- [ ] Implement ETL using Airflow (Extract → Transform → Load)
  - [ ] Incremental load (daily)
  - [ ] SCD Type 2 logic for dimensions
  - [ ] Error handling & error table (error_records)
- [ ] Build PowerBI Direct Query dashboards (7 dashboards)

---

### ✨ Current Status
| Step | Status |
|------|--------|
| Repo created | ⬜ *(will check when committed)* |
| Documentation | ⬜ |
| PostgreSQL source uploaded | ⬜ |
| DWH schema created | ⬜ |
| ETL started | ⬜ |
| ClickHouse working | ⬜ |
| PowerBI dashboards | ⬜ |

---

### 📁 Planned Repository Structure

/dwh-adventureworks/
|-- docs/
|-- sql/
|   |-- dim/
|   |-- fact/
|   |-- agg/
|-- airflow/
|-- clickhouse/
|-- postgres/
|-- README.md
