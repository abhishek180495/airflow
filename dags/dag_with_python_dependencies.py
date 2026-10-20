from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "coder2j",
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

def get_sklearn():
    import sklearn
    print(f"sklearn imported and version is : {sklearn.__version__}")

def get_matplotlib():
    import matplotlib
    print(f"matplotlib imported and version is : {matplotlib.__version__}")

with DAG(
    default_args= default_args,
    dag_id="dag_with_python_dependencies_v01",
    start_date=datetime(2026,10,15),
    schedule='@daily'
) as dag:
    get_sklearn_task=PythonOperator(
        task_id="get_sklearn_task",
        python_callable=get_sklearn
    )
    get_matplotlib_task=PythonOperator(
        task_id="get-matplotlib",
        python_callable=get_matplotlib
    )
    get_sklearn_task >> get_matplotlib_task
