from airflow.sdk.bases import xcom
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

import requests
import json
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.utils.timezone import utc

def product_page_callable(**context):
    ti=context['ti']
    ti.xcom_push(key="output_path",value="/airflow/output_files/folder1/test.csv")
    print("Pushed xcom with key 'output_path' and value is '/airflow/output_files/folder1/test.csv' ")

def read_raw_callable(**context):
    ti=context['ti']
    output_path=ti.xcom_pull(key="output_path",task_ids="product_page")
    print(f"Pulled xcom with key 'output_path' : {output_path}. ")

with DAG(dag_id="xcom_dag2",
         start_date=datetime(2026,6,21),
         schedule=CronDataIntervalTimetable(cron="0 0 * * *",timezone=utc),
         catchup=False
         )as dag:
    product_page=PythonOperator(
        dag=dag,
        task_id="product_page",
        python_callable=product_page_callable
    )
    read_raw=PythonOperator(
        dag=dag,
        task_id="read_raw",
        python_callable=read_raw_callable
    )

    product_page >> read_raw