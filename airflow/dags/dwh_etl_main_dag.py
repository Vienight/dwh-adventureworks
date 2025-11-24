"""
dwh_etl_main_dag.py
AdventureWorks DWH – Main ETL Orchestration DAG
Owner: @Vienight
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# Import your custom modules (make sure they are in airflow/python/ or in PYTHONPATH)
from python.extraction import extract_all_dimensions, extract_all_facts
from python.validation import run_data_quality_checks
from python.transformation import transform_and_scd2_merge
from python.loading import load_to_clickhouse, load_to_error_table
from python.error_handling import classify_and_log_errors, send_alert_if_critical
from python.utilities import get_last_successful_run, mark_run_success, mark_run_failed

# =============================================================================
# DAG Settings
# =============================================================================
default_args = {
    'owner': 'vienight',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'max_active_runs': 1,
}

dag = DAG(
    dag_id='adventureworks_dwh_full_load',
    default_args=default_args,
    description='Full daily ETL: PostgreSQL → Staging → SCD2 → ClickHouse + Error Handling',
    schedule_interval='0 2 * * *',           # Every day at 02:00 AM
    tags=['dwh', 'adventureworks', 'clickhouse', 'scd2'],
)

# =============================================================================
# Tasks
# =============================================================================

start = BashOperator(
    task_id='start',
    bash_command='echo "Starting AdventureWorks DWH ETL – $(date)"',
    dag=dag,
)

# 1. Extract all dimensions & facts from PostgreSQL (incremental where possible)
extract_dims = PythonOperator(
    task_id='extract_dimensions',
    python_callable=extract_all_dimensions,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
)

extract_facts = PythonOperator(
    task_id='extract_facts',
    python_callable=extract_all_facts,
    op_kwargs={'execution_date': '{{ ds }}'},
    dag=dag,
)

# 2. Data Quality Checks (nulls, negatives, FK, business rules)
validation = PythonOperator(
    task_id='data_quality_checks',
    python_callable=run_data_quality_checks,
    provide_context=True,
    dag=dag,
)

# 3. Branch: If critical errors → stop pipeline + alert
def check_if_critical_errors(**context):
    errors = context['ti'].xcom_pull(task_ids='data_quality_checks')
    critical = any(e['severity'] == 'CRITICAL' for e in errors)
    return 'send_critical_alert' if critical else 'transform_and_load'

branch_on_errors = BranchPythonOperator(
    task_id='critical_errors_branch',
    python_callable=check_if_critical_errors,
    provide_context=True,
    dag=dag,
)

send_critical_alert = PythonOperator(
    task_id='send_critical_alert',
    python_callable=send_alert_if_critical,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# 4. Transform + SCD Type 2 logic
transform = PythonOperator(
    task_id='transform_and_scd2',
    python_callable=transform_and_scd2_merge,
    provide_context=True,
    dag=dag,
)

# 5. Load to ClickHouse (with error capture)
load_clean = PythonOperator(
    task_id='load_clean_data_to_clickhouse',
    python_callable=load_to_clickhouse,
    op_kwargs={'data_type': 'clean'},
    dag=dag,
)

load_errors = PythonOperator(
    task_id='load_errors_to_error_table',
    python_callable=load_to_error_table,
    trigger_rule=TriggerRule.ALL_DONE,   # always run, even if previous failed
    dag=dag,
)

# 6. Final error classification & optional reprocessing
classify_errors = PythonOperator(
    task_id='classify_and_log_errors',
    python_callable=classify_and_log_errors,
    dag=dag,
)

# 7. Mark success / send summary
end_success = PythonOperator(
    task_id='mark_success_and_notify',
    python_callable=mark_run_success,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

end_failed = PythonOperator(
    task_id='mark_failed_and_notify',
    python_callable=mark_run_failed,
    trigger_rule=TriggerRule.ONE_FAILED,
    dag=dag,
)

# =============================================================================
# Task Dependencies
# =============================================================================

start >> [extract_dims, extract_facts]
[extract_dims, extract_facts] >> validation
validation >> branch_on_errors

branch_on_errors >> send_critical_alert >> end_failed
branch_on_errors >> transform >> load_clean >> classify_errors
load_clean >> load_errors   # errors still get loaded even if clean load succeeds

classify_errors >> end_success
load_errors >> end_success

# If any task fails → go to end_failed
[end_success, send_critical_alert] >> end_failed
