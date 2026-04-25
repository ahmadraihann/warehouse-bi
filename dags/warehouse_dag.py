from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Define the base directory (absolute path for reliability)
BASE_DIR = r"c:\LEARN\TUGAS KULIAH\DWIB\warehouse-bi"
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
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# 1. Ingest Data (Runs pipeline.py)
ingest_data = BashOperator(
    task_id='ingest_data',
    bash_command=f'python "{PIPELINE_SCRIPT}" --fresh',
    dag=dag,
)

# 2. Transform Data (Runs dbt run)
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'dbt run --project-dir "{DBT_DIR}" --profiles-dir "{DBT_DIR}"',
    dag=dag,
)

# 3. Test Data (Runs dbt test)
test_data = BashOperator(
    task_id='test_data',
    bash_command=f'dbt test --project-dir "{DBT_DIR}" --profiles-dir "{DBT_DIR}"',
    dag=dag,
)

# Set dependencies
ingest_data >> transform_data >> test_data
