from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def task_greet():
    print("Hello World!")


default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dag_edu04_python_operator",
    default_args=default_args,
    description="Python operator를 활용하는 DAG",
    start_date=datetime(2024, 5, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="task_edu4_python_operator", python_callable=task_greet
    )
