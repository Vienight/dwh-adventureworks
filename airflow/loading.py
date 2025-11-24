
from clickhouse_driver import Client
import logging
from utilities import get_db_connection, log_error

client = Client(host='clickhouse', port=9000, user='default', password='')

def load_to_clickhouse(df: pd.DataFrame, table_name: str, **context):
    try:
        client.execute(f"INSERT INTO {table_name} VALUES", df.to_dict('records'))
        logging.info(f"Loaded {len(df)} rows into {table_name}")
    except Exception as e:
        logging.error(f"Load failed for {table_name}: {e}")
        log_error(table_name, "LOAD", "CRITICAL", str(e))
        raise

def load_to_error_table(**context):
    # Placeholder – real implementation would pull from temp error DataFrame
    pass
