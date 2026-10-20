
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
from airflow.timetables.interval import CronDataIntervalTimetable

default_args = {
    "owner": "abhishek",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}



def postgres_to_s3(
    data_interval_start,
    data_interval_end,
    logical_date,
    run_id,
    dag_run,
):
    logging.info("Run ID: %s", run_id)
    logging.info("Run type: %s", dag_run.run_type)
    logging.info("Logical date: %s", logical_date)
    logging.info("Interval start: %s", data_interval_start)
    logging.info("Interval end: %s", data_interval_end)

    logging.info(
        "Interval duration: %s",
        data_interval_end - data_interval_start,
    )


with DAG(
    dag_id="dag_with_dates_check",
    start_date=datetime(2026, 10, 14),
    schedule=CronDataIntervalTimetable("29 23 * * *",timezone="Asia/Kolkata"),
    default_args=default_args,
    catchup=True,
) as dag:

    task1 = PythonOperator(
        task_id="date-check",
        python_callable=postgres_to_s3,
    )
    task1