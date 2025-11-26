# ETL Pipeline Documentation

## Architecture
1. **Source:** PostgreSQL AdventureWorks (landing zone).
2. **Airflow DAG (`airflow/dwh_etl_main_dag.py`):** orchestrates extract → validate → load dims → load facts → update aggregates → reprocess errors.
3. **Target:** ClickHouse star schema (see `sql` folder).
4. **Monitoring:** `error_records` MergeTree table + alert routing via Airflow email.

```
PostgreSQL → Extract → Validate → Load Dimensions (SCD1 & SCD2)
            → Load Facts → Update Aggregates → Reprocess Recoverable Errors
```

## Tasks
- `extract_incremental_data`: pulls incremental slices using `ModifiedDate` window and stores serialized DataFrames in XCom.
- `validate_extracted_data`: runs null/duplicate/range checks via `airflow/validation.py`.
- `load_dim_*_scd2`: executes SCD Type 2 diffing, expiring prior versions, and inserting new versions using `airflow/loading.py`.
- `load_fact_sales` (template for other facts): resolves surrogate keys and loads fact rows with FK validation.
- `update_aggregates`: recomputes `agg_daily_sales` for the processing date (extendable to weekly/monthly jobs).
- `reprocess_recoverable_errors`: scans `error_records` for `IsRecoverable=1` rows and retries.

## Scheduling
- DAG schedule: `0 1 * * *` (daily at 01:00 local Airflow time).
- Weekly/monthly aggregates triggered via same DAG using `processing_date` context to filter.

## Incremental Logic
- Extraction uses `utilities.determine_processing_window` to derive `[last_run, current_run]`.
- SCD2 detection compares incoming vs current ClickHouse snapshots and only re-loads changed members.
- Fact loads are append-only; inventory snapshots overwrite per-date partitions if necessary.
- Aggregates only recompute for the relevant date/week/month slice for efficiency.

## Dependencies & Config
- Connections derived from Airflow Variables (`pg_host`, `pg_user`, etc.).
- Python dependencies: `pandas`, `psycopg2`, `clickhouse-driver`, `pendulum`.
- Metadata stored under `metadata/last_run.json` for CDC windows.

## Testing Strategy
- **Unit tests:** Validate dataframe transforms (run locally with pytest).
- **Integration smoke test:** Run DAG for a known small processing date to ensure table-level counts.
- **Backfill:** Use Airflow backfill or manual parameterization; facts are append-only so safe to re-run.

## Deployment Steps
1. Apply ClickHouse DDLs from `sql/01-05_*.sql`.
2. Configure Airflow connections/variables.
3. Place modules under Airflow `dags/` directory (maintain package structure).
4. Trigger DAG with `airflow dags trigger dwh_etl_pipeline --conf '{"processing_date": "2025-01-01"}'`.
5. Monitor `error_records` and Airflow UI for run status.

