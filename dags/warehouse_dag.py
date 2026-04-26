from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Define the base directory (container path)
BASE_DIR = "/opt/airflow"
DBT_DIR = os.path.join(BASE_DIR, "dbt_transformasi_dan_pemodelan_data")
PIPELINE_SCRIPT = os.path.join(BASE_DIR, "syntetic_data_generation", "pipeline.py")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'warehouse_inventory_pipeline',
    default_args=default_args,
    description='Pipeline for Warehouse BI - Ingest and Build Data Mart',
    schedule=timedelta(days=1),
    catchup=False,
)

# 1. Ingest Data (Runs pipeline.py)
ingest_data = BashOperator(
    task_id='ingest_data',
    bash_command=f'ls -la "{BASE_DIR}/syntetic_data_generation" && python3 "{PIPELINE_SCRIPT}" --fresh',
    dag=dag,
)

# 2. Prepare dbt (Install dependencies like dbt_utils)
prepare_dbt = BashOperator(
    task_id='prepare_dbt',
    bash_command=f'cd "{DBT_DIR}" && dbt deps',
    dag=dag,
)

# 3. Snapshot Data (Runs dbt snapshot)
snapshot_data = BashOperator(
    task_id='snapshot_data',
    bash_command=f'cd "{DBT_DIR}" && dbt snapshot --profiles-dir .',
    dag=dag,
)

# 4. Transform Data (Runs dbt run)
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'cd "{DBT_DIR}" && dbt run --profiles-dir .',
    dag=dag,
)

# 5. Test Data (Runs dbt test)
test_data = BashOperator(
    task_id='test_data',
    bash_command=f'cd "{DBT_DIR}" && dbt test --profiles-dir .',
    dag=dag,
)

# Set dependencies
ingest_data >> prepare_dbt >> snapshot_data >> transform_data >> test_data
