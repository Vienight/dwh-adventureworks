
import pandas as pd
import logging
from sqlalchemy import create_engine
from utilities import get_db_connection, log_error

def extract_table(table_name: str, incremental_col: str = None, last_run_date: str = None) -> pd.DataFrame:
    """Universal extractor with incremental support"""
    conn = get_db_connection('postgresql')
    try:
        if incremental_col and last_run_date:
            query = f"SELECT * FROM {table_name} WHERE {incremental_col} > '{last_run_date}'"
            logging.info(f"Incremental extract from {table_name} since {last_run_date}")
        else:
            query = f"SELECT * FROM {table_name}"
            logging.info(f"Full extract from {table_name}")

        df = pd.read_sql(query, conn)
        logging.info(f"Extracted {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        log_error(table_name, "EXTRACTION", "CRITICAL", str(e))
        raise
    finally:
        conn.close()

def extract_all_dimensions(**context):
    dims = ['Customer', 'Product', 'Store', 'Employee', 'Promotion', 'Vendor']
    results = {}
    for dim in dims:
        df = extract_table(f"dim_{dim.lower()}", "ModifiedDate", context['ds'])
        results[f"dim_{dim.lower()}"] = df
        context['ti'].xcom_push(key=f"dim_{dim.lower()}", value=df.head(0).to_json())  # schema only
    return results

def extract_all_facts(**context):
    facts = ['SalesOrderHeader', 'SalesOrderDetail', 'PurchaseOrderHeader', 'InventoryTransaction']
    results = {}
    for fact in facts:
        df = extract_table(fact, "ModifiedDate", context['ds'])
        results[fact.lower()] = df
    return results
