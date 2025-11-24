# Internship Task: Building a Data Warehouse from Adventureworks Data
## Deliverables – Realistic Checkbox Status (as of 24.11.2025)

### 1. SQL Scripts for ClickHouse Table Creation
- [x] 01_create_dim_tables.sql – All dimension table definitions (SCD Type 1 & Type 2)  
- [x] 02_create_fact_tables.sql – All fact table definitions with appropriate engine  
- [x] 03_create_aggregate_tables.sql – Aggregate table and materialized view definitions  
- [x] 04_create_error_tables.sql – Error records and monitoring tables  
- [x] 05_create_indexes_and_partitioning.sql – Optimization scripts  


### 2. Apache Airflow DAG Python Scripts
- [x] dwh_etl_main_dag.py – Main DAG orchestrating all tasks (structure + dependencies perfect)  
- [ ] extraction.py – PostgreSQL data extraction functions with error handling  
- [ ] validation.py – Data quality validation functions  
- [ ] transformation.py – Data transformation and SCD logic (the most important one)  
- [ ] loading.py – ClickHouse load functions with error logging  
- [ ] error_handling.py – Error recording, classification, and reprocessing logic  
- [ ] utilities.py – Helper functions (logging, error handling, metadata mgmt, alerting)  


### 3. Documentation
- [ ] Star Schema Design Document (2–3 pages + ERD)  
- [ ] ETL Pipeline Documentation (3–4 pages)  
- [ ] Error Handling & Monitoring Guide (2–3 pages)  
- [ ] Operational Runbook (2–3 pages)  


### 4. Presentation/Demo
- [ ] 15–20 minute presentation + live demo  

- [ ] 15–20 minute presentation + live demo  

→ Only needed if they invite you for an interview
