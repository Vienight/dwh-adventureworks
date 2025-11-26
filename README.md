# AdventureWorks Data Warehouse

## Contents ##
- `airflow/` — Airflow DAG files and task modules.
- `data/` — PostgreSQL Database Samples, main focus on AdventureWorks
- `docs/` — Supplementary docs.
- `sql/` — SQL scripts to create tables, views and aggregates.

### Progress ###

**1. SQL Scripts for ClickHouse Table Creation**
- [x] 01_create_dim_tables.sql - All dimension table definitions (SCD Type 1 & Type 2)
- [x] 02_create_fact_tables.sql - All fact table definitions with appropriate engine
- [x] 03_create_aggregate_tables.sql - Aggregate table and materialized view definitions
- [x] 04_create_error_tables.sql - Error records and monitoring tables
- [x] 05_create_indexes_and_partitioning.sql - Optimization scripts

**2. Apache Airflow DAG Python Scripts**
- [x] dwh_etl_main_dag.py - Main DAG orchestrating all tasks
- [x] extraction.py - PostgreSQL data extraction functions with error handling
- [x] validation.py - Data quality validation functions
- [x] transformation.py - Data transformation and SCD logic
- [x] loading.py - ClickHouse load functions with error logging
- [x] error_handling.py - Error recording, classification, and reprocessing logic
- [x] utilities.py - Helper functions (logging, error handling, metadata mgmt, alerting)

**3. Documentation**
- [ ] Star Schema Design Document (2-3 pages):
   - Entity-Relationship Diagram (ERD)
   - Fact and dimension definitions with all attributes
   - SCD strategy per dimension
   - Grain description for each fact table

- [ ] ETL Pipeline Documentation (3-4 pages):
   - Data flow diagram
   - Task dependencies and scheduling
   - Error handling and recovery procedures
   - Change detection methodology
   - Performance optimization notes
   - Error records table structure and usage

- [ ] Error Handling & Monitoring Guide (2-3 pages):
   - Error classification framework
   - Recoverable vs. non-recoverable error definitions
   - Error reprocessing procedures
   - Dashboard queries for error monitoring
   - Escalation procedures for unresolved errors

- [ ] Operational Runbook (2-3 pages):
   - How to monitor pipeline health
   - How to check and resolve errors from error_records table
   - Common troubleshooting steps
   - How to manually backfill data
   - Contacts and escalation procedures
   - Error investigation and resolution workflows

**4. Presentation/Demo**
- [ ] 15-20 minute presentation covering:
   - Data warehouse design rationale
   - Error handling architecture and strategy
   - Live demo: Extract one day of data from PostgreSQL
   - Live demo: Execute Airflow DAG for that day
   - Show ClickHouse tables populated with data
   - Show error_records table with sample errors and resolution status
   - Run sample analytical queries against aggregates
   - Performance metrics: row counts, load times, query performance, error rates
   - Lessons learned and next steps
