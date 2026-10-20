from datetime import datetime, timedelta
from airflow import DAG
import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args={
    'owner': 'abhishek',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_to_s3():
    logging.info("----------inside postgres function----------------")
    #- step-1: query data from postgresql db and save text file in local
    hook=PostgresHook(postgres_conn_id = "postgres_localhost")
    conn=hook.get_conn()
    cursor=conn.cursor()
    cursor.execute("""
    select * from orders
    where date >= '20261015';
    """)
    with open("dags/get_orders.txt","w") as f:
        csv_writer=csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)

    cursor.close()
    conn.close()

    logging.info("Save order data in text file get_orders.csv------------")

    s3_hook=S3Hook(aws_conn_id= "minio_conn")
    s3_hook.load_file(
        filename="dags/get_orders.txt",
        key=f"data/get_orders.txt",
        bucket_name="airflow",
        replace=True
    )
    logging.info("-------data uploaded to s3/minio bucket successfully ")
    
with DAG(
    dag_id="dag_with_postgres_hooks_v2",
    start_date=datetime(2026,10,14),
    schedule='@daily',
    default_args= default_args
) as dag:
    task1=PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1
