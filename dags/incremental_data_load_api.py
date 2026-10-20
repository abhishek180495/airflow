import requests
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def fetch_api_data(**kwargs):
    url = "http://fastapi-app:5000/getAll"
    payload = json.dumps({
            "start_date": "2026-06-10",
            "end_date": "2026-06-10"
        })
    headers = {
      'accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': 'Basic YWRtaW46YWJoaXNoZWs='
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    data=response.json()
    output_path = "/opt/airflow/output_files/dag_tesult_20260610.json"
    i=0
    # print(f"{i+1}--- {response.text}")
    with open(output_path, "w") as file:
        json.dump(data,file,indent=4)
    print(f"Saved api output at : {output_path}")

def print_context(**kwargs):
    print(kwargs)
    print("--Task completed---")
with DAG(dag_id="API_Incremental_Data_Load",
         start_date=datetime(2026,6,1),
         schedule='0 * * * *',
         catchup=False) as dag:

    pull_api_data=PythonOperator(dag=dag,
                                 task_id="pull_api_data",
                                 python_callable=fetch_api_data
                                 )
    task2=PythonOperator(
        task_id="task2",
        python_callable=print_context
    )

    pull_api_data>>task2
    