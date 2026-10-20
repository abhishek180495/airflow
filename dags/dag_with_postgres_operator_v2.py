from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "coder2j",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

def print_func():
    print(f"Command run completed successfully!")

with DAG(
    dag_id="dag_with_postgres_operator_v02",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_postgres_tables_v2",
        conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt DATE,
                dag_id VARCHAR,
                PRIMARY KEY (dt, dag_id)
            );
        """,
    )

    insert_data=SQLExecuteQueryOperator(
        task_id="insert_postgres_tables_v2",
        conn_id="postgres_localhost",
        sql="""
        insert into dag_runs(dt, dag_id)
        values('2026-10-18','dag_v06');
        """,
    )

    print_func=PythonOperator(
        task_id="print_function",
        python_callable=print_func
    )
    create_table >> insert_data >> print_func