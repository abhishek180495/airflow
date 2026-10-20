from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_context(**kwargs):
    print(kwargs)
    print("task completed")

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False
    ) as dag:
    copy_file=BashOperator(
        task_id="copy_file",
        bash_command="echo copying file"

    )
    task2=PythonOperator(
        task_id="task2",
        python_callable=print_context
    )

    copy_file >> task2
