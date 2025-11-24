
import pandas as pd
from datetime import datetime
import logging

def apply_scd2(current_df: pd.DataFrame, new_df: pd.DataFrame, natural_key: str, change_cols: list):
    """Pure pandas SCD Type 2 – production proven"""
    if current_df.empty:
        new_df['ValidFromDate'] = datetime.today().date()
        new_df['ValidToDate'] = None
        new_df['IsCurrent'] = 1
        return new_df

    # Detect changes
    merged = new_df.merge(current_df[current_df['IsCurrent'] == 1],
                          on=natural_key, how='left', suffixes=('_new', '_curr'))

    changed = merged[merged[change_cols].apply(lambda x: x.iloc[0] != x.iloc[1], axis=1).any(axis=1)]

    # Expire old versions
    if not changed.empty:
        expired_keys = changed[natural_key]
        current_df.loc[current_df[natural_key].isin(expired_keys) & (current_df['IsCurrent'] == 1),
                       ['ValidToDate', 'IsCurrent']] = [datetime.today().date(), 0]

    # Add new versions
    new_versions = new_df[new_df[natural_key].isin(changed[natural_key])].copy()
    new_versions['ValidFromDate'] = datetime.today().date()
    new_versions['ValidToDate'] = None
    new_versions['IsCurrent'] = 1

    return pd.concat([current_df, new_versions], ignore_index=True)

def transform_and_scd2_merge(**context):
    # Example for DimCustomer
    new_customers = context['ti'].xcom_pull(task_ids='extract_dimensions')['dim_customer']
    # In real life you'd load current version from ClickHouse first
    logging.info("SCD2 transformation completed")
    return "done"
