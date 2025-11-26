# Operational Runbook

## Daily Monitoring Checklist
- Confirm `dwh_etl_pipeline` DAG success by 02:00; check task Gantt.
- Review Airflow logs for WARN/ERROR entries.
- Run ClickHouse freshness query:
  ```sql
  SELECT max(SalesDateKey) FROM FactSales;
  ```
- Check `error_records` for unresolved critical issues.

## Handling Failures
1. **Extract/Validate failures:** inspect Postgres connectivity; rerun task after verifying credentials.
2. **Dimension load failure:** confirm ClickHouse availability, verify schema drift, re-run individual task via Airflow UI.
3. **Fact load failure:** inspect `error_records` for details; fix upstream data then clear+rerun `load_fact_sales`.
4. **Aggregate failure:** rerun `update_aggregates`; safe because aggregates are idempotent per date.

## Reprocessing Errors
- Trigger `reprocess_recoverable_errors` task manually or run `airflow tasks run dwh_etl_pipeline reprocess_recoverable_errors <ds>`.
- Monitor retries via:
  ```sql
  SELECT ErrorID, RetryCount, ErrorMessage
  FROM error_records
  WHERE IsResolved = 0 AND RetryCount > 0
  ORDER BY RetryCount DESC;
  ```

## Backfill Procedure
1. Pause production DAG schedule if performing long backfill.
2. Run `airflow dags backfill dwh_etl_pipeline -s 2025-01-01 -e 2025-01-07`.
3. Verify date coverage via row counts and aggregates.
4. Resume schedule and monitor for late-arriving dimensions (FK misses should auto-resolve).

## Manual Data Fix
1. Correct data in PostgreSQL landing tables.
2. Clear affected Airflow task instances.
3. Optionally run targeted SQL on ClickHouse (e.g., delete bad partition) before reloading.
4. Document fix in `ResolutionComment`.

## Escalation Contacts
- Data Engineering On-Call: data-warehouse@company.com
- DBA Team: dba-support@company.com
- Business Owner: analytics-product@company.com

## Useful Commands
- `airflow dags list` – verify DAG is deployed.
- `airflow dags state dwh_etl_pipeline <execution_date>` – run status.
- `clickhouse-client --query "SYSTEM PARTS"` – storage diagnostics.
- `psql -f scripts/load_dimensions.sql` – manual seeding when needed.

