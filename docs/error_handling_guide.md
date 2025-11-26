# Error Handling & Monitoring Guide

## Error Classification
| Type | Recoverable | Example | Action |
|------|-------------|---------|--------|
| Foreign key miss | Yes | Customer not yet loaded | Log row, retry once prerequisites land |
| Null PK | No | Source missing primary key | Stop load, alert immediately |
| Data type mismatch | Sometimes | Non-numeric revenue field | Attempt cast → log → retry up to 3x |
| Duplicate natural key | Yes | Multiple updates same day | Keep latest, log warning |
| Connection timeout | Yes | DB outage | Automatic exponential retry (60/120/240s) |
| Schema mismatch | No | Unexpected column drop | Fail fast, manual remediation |

## Error Recording
- Implemented via ClickHouse table `error_records` (see `sql/04_create_error_tables.sql`).
- Captures metadata such as `ErrorType`, `Severity`, serialized `FailedData`, and `ProcessingBatchID`.
- Write helper `error_handling.log_error_record` ensures consistent schema.

## Retry Workflow
1. `load_fact_table` (and other loaders) flag recoverable issues with `IsRecoverable=1`.
2. `reprocess_recoverable_errors` task fetches up to 100 pending issues ordered by `LastAttemptDate`.
3. Successful retries update `IsResolved`, otherwise increment `RetryCount`.
4. Records exceeding retry budget require manual follow-up via runbook.

## Alerting
- Airflow default email triggers on DAG failure.
- Add optional Slack/Webhook integration calling `utilities.get_logger`.
- Daily digest query example:
  ```sql
  SELECT ErrorType, COUNT(*) AS total, SUM(IsRecoverable) AS recoverable
  FROM error_records
  WHERE ErrorDate >= today() - 7
  GROUP BY ErrorType
  ORDER BY total DESC;
  ```

## Dashboards
- Build Grafana/Superset panels on top of `error_monitoring_summary`.
- Key visuals: trending error volume, unresolved critical list, retry effectiveness.

## Manual Resolution Steps
1. Inspect `FailedData` JSON; reproduce query in source system.
2. Fix upstream data or insert missing dimension members.
3. Update `error_records` row with `ResolutionComment` and `IsResolved=1`.
4. Trigger targeted backfill using Airflow `clear` or custom CLI.

## Data Retention
- TTL configured to delete resolved records after 90 days (see `sql/05_create_indexes_and_partitioning.sql`).
- Archive unresolved critical records quarterly to off-line storage if needed.

