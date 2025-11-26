"""
Extraction logic for AdventureWorks source in PostgreSQL.
"""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, Optional

import pandas as pd

from utilities import (
    PostgresConfig,
    dataframe_from_query,
    determine_processing_window,
    get_logger,
    get_postgres_conn,
    load_last_run_time,
    log_row_counts,
    save_last_run_time,
)

LOGGER = get_logger("extraction")


DIMENSION_TABLES = {
    "customer": "sales.customer",
    "product": "production.product",
    "store": "sales.store",
    "employee": "humanresources.employee",
    "vendor": "purchasing.vendor",
}

FACT_TABLES = {
    "FactSales": "sales.salesorderdetail",
    "FactPurchases": "purchasing.purchaseorderdetail",
    "FactInventory": "production.productinventory",
    "FactReturns": "sales.salesorderheadersalesreason",
}


def extract_incremental_data(
    processing_date: str,
    pg_config: PostgresConfig,
    tables: Optional[Iterable[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Pull incremental data for the provided processing date.
    """
    LOGGER.info("Starting extraction for %s", processing_date)
    selected_tables = tables or list(DIMENSION_TABLES.keys()) + list(FACT_TABLES.keys())

    last_run = load_last_run_time("extraction")
    window = determine_processing_window(last_run, datetime.fromisoformat(f"{processing_date}T00:00:00"))

    payload: Dict[str, pd.DataFrame] = {}
    with get_postgres_conn(pg_config) as conn:
        for name in selected_tables:
            query = _build_query(name, window)
            df = dataframe_from_query(conn, query)
            log_row_counts(LOGGER, f"extracted_{name}", df)
            payload[name] = df

    save_last_run_time("extraction", datetime.utcnow())
    return payload


def _build_query(table_name: str, window: Dict[str, datetime]) -> str:
    if table_name in DIMENSION_TABLES:
        source = DIMENSION_TABLES[table_name]
        return (
            f"SELECT * FROM {source} "
            f"WHERE COALESCE(modifieddate, now()) BETWEEN '{window['from']}' AND '{window['to']}'"
        )

    source = FACT_TABLES.get(table_name)
    if not source:
        raise ValueError(f"Unknown table {table_name}")
    process_date = window["to"].date()
    return (
        f"SELECT * FROM {source} "
        f"WHERE CAST('{process_date}' AS DATE) = CAST(modifieddate AS DATE)"
    )


