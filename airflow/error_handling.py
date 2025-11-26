"""
Centralized error handling utilities.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable

from clickhouse_driver import Client

from utilities import get_logger

LOGGER = get_logger("error_handling")


def log_error_record(
    client: Client,
    error_type: str,
    error_message: str,
    failed_data: Dict[str, Any],
    source_table: str,
    natural_key: str,
    processing_batch_id: str,
    task_name: str,
    is_recoverable: bool,
) -> None:
    record = {
        "ErrorID": int(datetime.utcnow().timestamp() * 1000),
        "ErrorDate": datetime.utcnow(),
        "SourceTable": source_table,
        "RecordNaturalKey": natural_key,
        "ErrorType": error_type,
        "ErrorSeverity": "Critical" if not is_recoverable else "Warning",
        "ErrorMessage": error_message,
        "ErrorDetails": "",
        "FailedData": json.dumps(failed_data),
        "ProcessingBatchID": processing_batch_id,
        "TaskName": task_name,
        "IsRecoverable": int(is_recoverable),
        "RetryCount": 0,
        "LastAttemptDate": datetime.utcnow(),
        "IsResolved": 0,
        "ResolutionComment": None,
    }
    client.insert("error_records", [record])
    LOGGER.warning("Logged error %s for %s", error_type, natural_key)


def reprocess_recoverable_errors(client: Client, limit: int = 100) -> None:
    query = """
        SELECT *
        FROM error_records
        WHERE IsResolved = 0
          AND IsRecoverable = 1
          AND RetryCount < 3
        ORDER BY LastAttemptDate ASC
        LIMIT %(limit)s
    """
    rows = client.execute(query, {"limit": limit})
    for row in rows:
        _retry_error(client, row)


def _retry_error(client: Client, row: Dict[str, Any]) -> None:
    LOGGER.info("Retrying error %s", row["ErrorID"])
    try:
        # Placeholder for real retry logic
        client.execute(
            """
            ALTER TABLE error_records
            UPDATE IsResolved = 1, ResolutionComment = %(comment)s
            WHERE ErrorID = %(id)s
            """,
            {"comment": "Auto-resolved during retry", "id": row["ErrorID"]},
        )
    except Exception as exc:  # pylint: disable=broad-except
        LOGGER.error("Retry failed for %s: %s", row["ErrorID"], exc)
        client.execute(
            """
            ALTER TABLE error_records
            UPDATE RetryCount = RetryCount + 1, LastAttemptDate = %(attempt)s
            WHERE ErrorID = %(id)s
            """,
            {"attempt": datetime.utcnow(), "id": row["ErrorID"]},
        )


