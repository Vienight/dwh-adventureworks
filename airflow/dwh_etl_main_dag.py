"""
Main Airflow DAG orchestrating the DWH pipeline.
"""

from __future__ import annotations

import os
import sys

DAG_DIR = os.path.dirname(os.path.realpath(__file__))
if DAG_DIR not in sys.path:
    sys.path.append(DAG_DIR)

from datetime import datetime, timedelta
from typing import Dict

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

import error_handling
import extraction
import loading
import validation
from utilities import (
    ClickHouseConfig,
    PostgresConfig,
    get_clickhouse_client,
    get_processing_batch_id,
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email": [],
    "email_on_failure": False,
    "start_date": datetime(2025, 1, 1),
}

PG_CONFIG = PostgresConfig(
    host=Variable.get("pg_host"),
    port=int(Variable.get("pg_port", default_var=5432)),
    database=Variable.get("pg_db"),
    user=Variable.get("pg_user"),
    password=Variable.get("pg_password"),
)

CH_CONFIG = ClickHouseConfig(
    host=Variable.get("ch_host"),
    port=int(Variable.get("ch_port", default_var=8123)),
    user=Variable.get("ch_user"),
    password=Variable.get("ch_password", default_var=""),
    database=Variable.get("ch_db"),
)


def _extract(**context):
    ti = context["ti"]
    processing_date = context["ds"]
    frames = extraction.extract_incremental_data(processing_date, PG_CONFIG)
    ti.xcom_push(
        key="frames",
        value={k: v.to_json(orient="records") for k, v in frames.items()},
    )


def _frames_from_xcom(context) -> Dict[str, pd.DataFrame]:
    ti = context["ti"]
    frames_json = ti.xcom_pull(task_ids="extract_incremental_data", key="frames") or {}
    return {k: pd.read_json(v) for k, v in frames_json.items()}


def _validate(**context):
    frames = _frames_from_xcom(context)
    return validation.validate_extracted_data(frames)


def _load_dim_customer(**context):
    frames = _frames_from_xcom(context)
    customer_df = frames.get("customer", pd.DataFrame())
    current = _fetch_clickhouse_df("SELECT * FROM DimCustomer WHERE IsCurrent = 1")
    loading.load_dimension_scd2(
        "DimCustomer",
        current,
        customer_df,
        natural_key="CustomerID",
        tracked_columns={
            "CustomerName": "string",
            "Email": "string",
            "City": "string",
            "Country": "string",
            "CustomerSegment": "string",
            "AccountStatus": "string",
        },
        processing_date=context["ds"],
        ch_config=CH_CONFIG,
    )


def _load_dim_product(**context):
    frames = _frames_from_xcom(context)
    product_df = frames.get("product", pd.DataFrame())
    current = _fetch_clickhouse_df("SELECT * FROM DimProduct WHERE IsCurrent = 1")
    loading.load_dimension_scd2(
        "DimProduct",
        current,
        product_df,
        natural_key="ProductID",
        tracked_columns={
            "ListPrice": "decimal",
            "Cost": "decimal",
            "Category": "string",
            "ProductStatus": "string",
        },
        processing_date=context["ds"],
        ch_config=CH_CONFIG,
    )


def _load_dim_store(**context):
    frames = _frames_from_xcom(context)
    store_df = frames.get("store", pd.DataFrame())
    current = _fetch_clickhouse_df("SELECT * FROM DimStore WHERE IsCurrent = 1")
    loading.load_dimension_scd2(
        "DimStore",
        current,
        store_df,
        natural_key="StoreID",
        tracked_columns={
            "Address": "string",
            "Region": "string",
            "Territory": "string",
            "ManagerName": "string",
            "StoreStatus": "string",
        },
        processing_date=context["ds"],
        ch_config=CH_CONFIG,
    )


def _load_dim_employee(**context):
    frames = _frames_from_xcom(context)
    employee_df = frames.get("employee", pd.DataFrame())
    current = _fetch_clickhouse_df("SELECT * FROM DimEmployee WHERE IsCurrent = 1")
    loading.load_dimension_scd2(
        "DimEmployee",
        current,
        employee_df,
        natural_key="EmployeeID",
        tracked_columns={
            "JobTitle": "string",
            "Department": "string",
            "Region": "string",
            "Territory": "string",
            "SalesQuota": "decimal",
        },
        processing_date=context["ds"],
        ch_config=CH_CONFIG,
    )


def _load_fact_sales(**context):
    frames = _frames_from_xcom(context)
    fact_df = frames.get("FactSales", pd.DataFrame())
    lookup_maps = {
        "customer": _build_lookup_map("DimCustomer", "CustomerKey", "CustomerID"),
        "product": _build_lookup_map("DimProduct", "ProductKey", "ProductID"),
        "store": _build_lookup_map("DimStore", "StoreKey", "StoreID"),
        "employee": _build_lookup_map("DimEmployee", "EmployeeKey", "EmployeeID"),
    }
    fk_columns = {
        "customer": "CustomerKey",
        "product": "ProductKey",
        "store": "StoreKey",
        "employee": "EmployeeKey",
    }
    batch_id = get_processing_batch_id(context["ds"], "sales")
    loading.load_fact_table(
        "FactSales",
        fact_df,
        lookup_maps,
        fk_columns,
        CH_CONFIG,
        batch_id,
    )


def _update_aggregates(**context):
    loading.update_aggregates(context["ds"], CH_CONFIG)


def _reprocess_errors(**context):
    client = get_clickhouse_client(CH_CONFIG)
    error_handling.reprocess_recoverable_errors(client)


def _fetch_clickhouse_df(query: str) -> pd.DataFrame:
    client = get_clickhouse_client(CH_CONFIG)
    data, columns = client.execute(query, with_column_types=True)
    column_names = [col[0] for col in columns]
    return pd.DataFrame(data, columns=column_names)


def _build_lookup_map(table: str, surrogate: str, natural: str) -> Dict[int, int]:
    df = _fetch_clickhouse_df(f"SELECT {surrogate}, {natural} FROM {table} WHERE IsCurrent = 1")
    if df.empty:
        return {}
    return dict(zip(df[natural], df[surrogate]))


with DAG(
    dag_id="dwh_etl_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 1 * * *",
    catchup=False,
    description="Incremental DWH Load from PostgreSQL to ClickHouse",
) as dag:
    extract_task = PythonOperator(
        task_id="extract_incremental_data",
        python_callable=_extract,
        provide_context=True,
    )

    validate_task = PythonOperator(
        task_id="validate_extracted_data",
        python_callable=_validate,
        provide_context=True,
    )

    load_dim_customer_task = PythonOperator(
        task_id="load_dim_customer_scd2",
        python_callable=_load_dim_customer,
        provide_context=True,
    )

    load_dim_product_task = PythonOperator(
        task_id="load_dim_product_scd2",
        python_callable=_load_dim_product,
        provide_context=True,
    )

    load_dim_store_task = PythonOperator(
        task_id="load_dim_store_scd2",
        python_callable=_load_dim_store,
        provide_context=True,
    )

    load_dim_employee_task = PythonOperator(
        task_id="load_dim_employee_scd2",
        python_callable=_load_dim_employee,
        provide_context=True,
    )

    load_fact_sales_task = PythonOperator(
        task_id="load_fact_sales",
        python_callable=_load_fact_sales,
        provide_context=True,
    )

    update_aggregates_task = PythonOperator(
        task_id="update_aggregates",
        python_callable=_update_aggregates,
        provide_context=True,
    )

    reprocess_errors_task = PythonOperator(
        task_id="reprocess_recoverable_errors",
        python_callable=_reprocess_errors,
        provide_context=True,
    )

    extract_task >> validate_task
    validate_task >> [
        load_dim_customer_task,
        load_dim_product_task,
        load_dim_store_task,
        load_dim_employee_task,
    ]
    [
        load_dim_customer_task,
        load_dim_product_task,
        load_dim_store_task,
        load_dim_employee_task,
    ] >> load_fact_sales_task
    load_fact_sales_task >> update_aggregates_task >> reprocess_errors_task


