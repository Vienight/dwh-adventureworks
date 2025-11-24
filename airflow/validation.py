
import pandas as pd
import logging
from utilities import log_error

def check_nulls(df: pd.DataFrame, table_name: str, critical_cols: list):
    nulls = df[critical_cols].isnull().sum()
    for col, count in nulls.items():
        if count > 0:
            log_error(table_name, "NULL_CHECK", "CRITICAL" if col in critical_cols else "WARNING",
                     f"{count} nulls in critical column {col}")

def check_negatives(df: pd.DataFrame, cols: list, table_name: str):
    for col in cols:
        if (df[col] < 0).any():
            bad_rows = df[df[col] < 0].shape[0]
            log_error(table_name, "NEGATIVE_VALUE", "CRITICAL", f"{bad_rows} negative values in {col}")

def run_data_quality_checks(**context):
    errors = []
    # Example: validate sales fact
    sales_df = context['ti'].xcom_pull(task_ids='extract_facts')['salesorderdetail']
    check_nulls(sales_df, "FactSales", ['OrderQty', 'UnitPrice'])
    check_negatives(sales_df, ['OrderQty', 'UnitPrice', 'LineTotal'], "FactSales")
    return errors
