from datetime import datetime, timedelta
from airflow import DAG
import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.timetables.interval import CronDataIntervalTimetable

default_args={
    'owner': 'abhishek',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def postgres_to_s3(data_interval_start, data_interval_end ):
    logging.info(f"dates handling------{data_interval_start}---------")

    # start_date_val,end_date_val=convert_dates(data_interval_start,data_interval_end )

    start_date_val=data_interval_start.strftime("%Y%m%d")
    end_date_val=data_interval_end.strftime("%Y%m%d")

    #- step-1: query data from postgresql db and save text file in local
    hook=PostgresHook(postgres_conn_id = "postgres_localhost")
    conn=hook.get_conn()
    cursor=conn.cursor()
    cursor.execute(f"""
    select * from orders where date >= '{start_date_val}' and date < '{end_date_val}';
    """)
    with open(f"dags/get_orders_{start_date_val}.txt","w") as f:
        csv_writer=csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerow(cursor)

    cursor.close()
    conn.close()

    logging.info("Save order data in text file get_orders.csv------------")

with DAG(
    dag_id="dag_with_postgres_hooks_test_v1",
    start_date=datetime(2026,10,14),
    schedule=CronDataIntervalTimetable("29 23 * * *",timezone="Asia/Kolkata"),
    default_args= default_args
) as dag:
    task1=PythonOperator(
        task_id="postgres_to_local",
        python_callable=postgres_to_s3
    )
    task1
