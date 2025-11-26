"""
Loading logic for ClickHouse targets.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, Tuple

import pandas as pd

from error_handling import log_error_record
from transformation import build_fact_payload, detect_scd2_changes
from utilities import ClickHouseConfig, get_clickhouse_client, get_logger

LOGGER = get_logger("loading")


def load_dimension_scd2(
    dimension: str,
    current_df: pd.DataFrame,
    incoming_df: pd.DataFrame,
    natural_key: str,
    tracked_columns: Dict[str, str],
    processing_date: str,
    ch_config: ClickHouseConfig,
) -> Tuple[int, int]:
    """
    Apply SCD type 2 logic.
    """
    diffs = detect_scd2_changes(current_df, incoming_df, natural_key, list(tracked_columns.keys()))
    inserted = _insert_dimension_rows(dimension, diffs.inserts, processing_date, ch_config)
    updated = _expire_dimension_rows(dimension, diffs.updates, processing_date, ch_config, natural_key)
    LOGGER.info("Dimension %s load complete inserted=%s updated=%s", dimension, inserted, updated)
    return inserted, updated


def load_dimension_scd1(
    dimension: str,
    incoming_df: pd.DataFrame,
    ch_config: ClickHouseConfig,
) -> int:
    LOGGER.info("Upserting SCD1 dimension %s", dimension)
    client = get_clickhouse_client(ch_config)
    rows = incoming_df.to_dict("records")
    if rows:
        client.insert(dimension, rows)
    return len(rows)


def load_fact_table(
    fact_name: str,
    fact_df: pd.DataFrame,
    lookup_maps: Dict[str, Dict[int, int]],
    fk_columns: Dict[str, str],
    ch_config: ClickHouseConfig,
    processing_batch_id: str,
) -> Tuple[int, int]:
    """
    Load fact data, tracking failed rows.
    """
    if fact_df.empty:
        return 0, 0
    enriched = build_fact_payload(fact_name, fact_df, lookup_maps, fk_columns)
    success_rows = []
    error_rows = 0
    client = get_clickhouse_client(ch_config)

    for idx, row in enriched.iterrows():
        if row.isnull().any():
            log_error_record(
                client=client,
                error_type="ForeignKeyMissing",
                error_message=f"Null FK in {fact_name}",
                failed_data=row.to_dict(),
                source_table=fact_name,
                natural_key=str(idx),
                processing_batch_id=processing_batch_id,
                task_name="load_fact_tables",
                is_recoverable=True,
            )
            error_rows += 1
            continue
        success_rows.append(row.to_dict())

    if success_rows:
        client.insert(fact_name, success_rows)
    return len(success_rows), error_rows


def update_aggregates(processing_date: str, ch_config: ClickHouseConfig) -> None:
    client = get_clickhouse_client(ch_config)
    query = """
        INSERT INTO agg_daily_sales
        SELECT
            SalesDateKey,
            StoreKey,
            ProductCategoryKey,
            SUM(SalesAmount),
            SUM(Quantity),
            SUM(DiscountAmount),
            COUNT()
        FROM FactSales
        WHERE SalesDateKey = toUInt32(toDate(%(date)s))
        GROUP BY SalesDateKey, StoreKey, ProductCategoryKey
    """
    client.execute(query, {"date": processing_date})
    LOGGER.info("Aggregates updated for %s", processing_date)


def _insert_dimension_rows(
    dimension: str,
    df: pd.DataFrame,
    processing_date: str,
    ch_config: ClickHouseConfig,
) -> int:
    if df.empty:
        return 0
    df = df.copy()
    df["ValidFromDate"] = processing_date
    df["ValidToDate"] = None
    df["IsCurrent"] = 1
    client = get_clickhouse_client(ch_config)
    client.insert(dimension, df.to_dict("records"))
    return len(df)


def _expire_dimension_rows(
    dimension: str,
    df: pd.DataFrame,
    processing_date: str,
    ch_config: ClickHouseConfig,
    natural_key: str,
) -> int:
    if df.empty:
        return 0
    client = get_clickhouse_client(ch_config)
    expire_date = datetime.fromisoformat(processing_date) - timedelta(days=1)
    for _, row in df.iterrows():
        client.execute(
            f"ALTER TABLE {dimension} UPDATE ValidToDate=%(date)s, IsCurrent=0 WHERE {natural_key}=%(key)s AND IsCurrent=1",
            {"date": expire_date.date(), "key": row[natural_key]},
        )
    return len(df)


