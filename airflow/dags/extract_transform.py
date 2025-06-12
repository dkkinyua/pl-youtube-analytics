import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.extract_data import extract_task
from etl.transform_and_load import transform_task

default_args = {
    'owner': 'deecodes',
    'retries': 5,
    'retry_delay': timedelta(minutes=3),
    'email_on_retry': False, # don't email after every retry
    'depends_on_past': False,
    'email': ['denzelkinyua11@gmail.com'],
    'email_on_failure': True # only email after failure
}

with DAG(
    dag_id='pl_analytics_dag',
    description='This DAG contains the pipeline for extracting, transforming and loading data from the Youtube API',
    default_args=default_args,
    start_date=datetime(2025, 6, 12),
    schedule='@weekly',
    catchup=False
) as dag:
    extract = PythonOperator(
        task_id = 'extract_data',
        python_callable=extract_task
    )

    transform = PythonOperator(
        task_id = 'transform_and_load',
        python_callable=transform_task
    )

    extract >> transform

