from datetime import datetime, timedelta
from airflow import DAG
import logging
import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from tempfile import NamedTemporaryFile
from airflow.timetables.interval import CronDataIntervalTimetable

default_args={
    'owner': 'abhishek',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
#   ---{{ data_interval_start }}  {{ data_interval_end }}


# def convert_dates(ab ,cd ):
#     end_date_val=cd.strftime("%Y%m%d")
#     start_date_val=ab.split('__')[1]
#     start_date_val=datetime.strptime(start_date_val,"%Y-%m-%dT%H:%M:%S%z").strftime("%Y%m%d")

#     logging.info(f"--------start-date is : {ab} and end_date is {end_date_val}  -----------")

#     return (start_date_val,end_date_val)

def postgres_to_s3(data_interval_start, data_interval_end ):
    logging.info(f"dates handling------{data_interval_start}---------")

    # start_date_val,end_date_val=convert_dates(data_interval_start,data_interval_end )

    start_date_val=data_interval_start.strftime("%Y%m%d")
    end_date_val=data_interval_end.strftime("%Y%m%d")

    logging.info("----------inside postgres function----------------")
    #- step-1: query data from postgresql db and save text file in local
    hook=PostgresHook(postgres_conn_id = "postgres_localhost")
    conn=hook.get_conn()
    cursor=conn.cursor()
    cursor.execute(f"select * from orders where date >= '{start_date_val}' and date < '{end_date_val}';")

    with NamedTemporaryFile(
    mode="w",
    prefix="orders_",
    suffix=f"_{start_date_val}.csv",
    delete=False,
    newline="",
    encoding="utf-8",
    ) as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow([col[0] for col in cursor.description])
        rows = cursor.fetchall()
        logging.info(f"--------------{rows}--------------------------")
        csv_writer.writerows(rows)
        temp_filename=f.name

    cursor.close()
    conn.close()
    
    # with open(temp_filename, "r", encoding="utf-8") as f:
    #     logging.info("CSV preview:\n%s", f.read()[:1000])

    logging.info(f" Save order data in text file get_orders.csv-----{f.name} \n and---- date-interval-start: {start_date_val}"
                 f"and --- date-interval-end {end_date_val}-------")
    s3_hook=S3Hook(aws_conn_id= "minio_conn")
    s3_hook.load_file(
        filename=temp_filename,
        key=f"data/orders_{start_date_val}.txt",
        bucket_name="airflow",
        replace=True
    )
    logging.info(f"-------data uploaded to s3/minio bucket successfully -----{f.name}")

with DAG(
    dag_id="dag_with_postgres_hooks_v6",
    start_date=datetime(2026,10,14),
    schedule=CronDataIntervalTimetable("29 23 * * *",timezone="Asia/Kolkata"),
    default_args= default_args,
    catchup=True
) as dag:
    task1=PythonOperator(
        task_id="postgres_to_s3",
        python_callable=postgres_to_s3
    )
    task1
