
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection(db_type: str):
    connections = {
        'postgresql': 'postgresql://user:pass@postgres:5432/adventureworks',
        'clickhouse': None  # handled separately
    }
    return create_engine(connections[db_type])

def log_error(source: str, error_type: str, severity: str, message: str):
    log_line = f"{source} | {error_type} | {severity} | {message}"
    logging.error(log_line)
