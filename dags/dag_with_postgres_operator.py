from datetime import datetime, timedelta
from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
import airflow.providers.postgres as p
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def get_postgres():
    print(f"Postgres operator version {p.__version__}")

with DAG(
    dag_id='dag_with_postgres_operator_v01',
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule='@daily',
    catchup=False
) as dag:
    get_postgres=PythonOperator(
        task_id="get_postgres-task",
        python_callable=get_postgres
    )
    get_postgres