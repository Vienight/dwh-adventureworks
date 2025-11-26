"""
Utility helpers shared across the DWH Airflow project.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import pendulum
import psycopg2
import psycopg2.extras
from clickhouse_driver import Client as ClickHouseClient


@dataclass
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass
class ClickHouseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


def get_logger(name: str = "dwh_etl") -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    return logging.getLogger(name)


def get_postgres_conn(cfg: PostgresConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def get_clickhouse_client(cfg: ClickHouseConfig) -> ClickHouseClient:
    return ClickHouseClient(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        send_receive_timeout=60,
    )


def get_processing_batch_id(processing_date: str, suffix: Optional[str] = None) -> str:
    base = f"{processing_date.replace('-', '')}"
    if suffix:
        return f"{base}_{suffix}"
    return base


METADATA_DIR = Path(os.getenv("DWH_METADATA_DIR", "metadata"))
METADATA_DIR.mkdir(exist_ok=True)
LAST_RUN_FILE = METADATA_DIR / "last_run.json"


def load_last_run_time(task_name: str) -> Optional[datetime]:
    if not LAST_RUN_FILE.exists():
        return None
    with LAST_RUN_FILE.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    ts = payload.get(task_name)
    return pendulum.parse(ts) if ts else None


def save_last_run_time(task_name: str, dt: datetime) -> None:
    payload: Dict[str, str] = {}
    if LAST_RUN_FILE.exists():
        with LAST_RUN_FILE.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    payload[task_name] = dt.isoformat()
    with LAST_RUN_FILE.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def dataframe_from_query(conn, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def log_row_counts(logger: logging.Logger, label: str, df: pd.DataFrame) -> None:
    logger.info("%s rowcount=%s columns=%s", label, len(df), list(df.columns))


def determine_processing_window(last_run: Optional[datetime], current: datetime) -> Dict[str, datetime]:
    if not last_run:
        last_run = current - timedelta(days=1)
    return {"from": last_run, "to": current}


def ensure_dataframe(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    return df


