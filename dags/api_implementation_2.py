from airflow.sdk import dag, task
from airfloe.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.models import Variable
from airflow.utils.timezone import utc
from airflow.timetable.interval import CronDataIntervalTimetable
from datetime import datetime
from include.api_client import call_api
from include.utils import write_json
from include.business_logic import merge_files, calculate_checkout_amount

auth_header=Variable.get("API_AUTH_HEADER")
base_url=Variable.get("BASE_API_URL")

@dag(
    dag_id="api_implementation_dag_12",
    start_date=datetime(2026,7,1,tzinfo=utc),
    schedule=CronDataIntervalTimetable("0 0 * * *",timezone=utc),
    catchup=False
)
def pipeline():
    start=EmptyOperator(task_id="start")
    end=EmptyOperator(task_id="end", trigger_rule="none_failed")

    @task
    def fetch_login(ds):
        data=call_api(f"{base_url}/loginUsers", ds, ds, auth_header)
        path=f"/opt/airflow/output_files/login/{ds}.json"
        write_json(data,path)
        return path
    
    @task
    def fetch_product(ds):
        data=call_api(f"{base_url}/productUsers",ds, ds, auth_header)
        path=f"/opt/airflow/output_files/product/{ds}.json"
        write_json(data,path)
        return path

    @task
    def fetch_checkout(ds):
        data=call_api(f"{base_url}/checkoutUsers",ds, ds, auth_header)
        path=f"/opt/airflow/output_files/checkout/{ds}.json"
        write_json(data,path)
        return path
    
    @task
    def merge_data(login_path, product_path, checkout_path, ds):
        merged_path=f"/opt/airflow/output_files/merged/{ds}.json"
        return merge_files(login_path, product_path. checkout_path, merged_path)
    
    @task.branch
    def branch_on_amount(checkout_path):
        total=calculate_checkout_amount(checkout_path)
        if total <= 0:
            return "alarming_situation"
        return "merge_data"
    alarming_situation=BashOperator(
        task_id="alarming_situation",
        bash_operator='echo "Alarming situation: Checkout amount is 0 for {{ ds }}',
    )
    notify_team=BashOperator(
        task_id="notify_team",
        bash_operator='echo "Daily API Pipeline Success: Merged data generated successfully for {{ ds }}"',
    )

    login=fetch_login()
    product=fetch_product()
    checkout=fetch_checkout()

    decision=branch_on_amount(checkout)
    merged=merge_data(login,product,checkout)
    start >> [login,product,checkout]
    [login, product, checkout] >> decision
    decision >> [alarming_situation , merged]
    merged >> notify_team
    [alarming_situation, notify_team] >>end

    dag=pipeline()

