
import logging
from loading import client

ERROR_TABLE = "error_records"

def classify_and_log_errors(**context):
    # Simulate error classification
    error_example = {
        'ErrorID': 123,
        'SourceTable': 'FactSales',
        'ErrorType': 'NEGATIVE_PRICE',
        'ErrorSeverity': 'CRITICAL',
        'ErrorMessage': 'UnitPrice < 0 detected',
        'IsRecoverable': 0,
        'IsResolved': 0
    }
    client.execute(f"INSERT INTO {ERROR_TABLE} VALUES", [error_example])
    logging.info("Error logged and classified")

def send_alert_if_critical(**context):
    logging.critical("CRITICAL ERRORS DETECTED – PIPELINE STOPPED")
    # Here you’d send Slack/Email/Teams alert
