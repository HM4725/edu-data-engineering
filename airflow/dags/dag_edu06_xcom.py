import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator


def get_name():
    return "hm"


def get_age():
    return 27


def task_greet(ti: TaskInstance):
    name = ti.xcom_pull(task_ids="get_name")
    age = ti.xcom_pull(task_ids="get_age")
    print(f"Hello World! My name is {name} and I am {age} years old.")


dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="xcom을 활용한 task간의 데이터 전달",
    start_date=datetime(2024, 5, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(task_id="get_name", python_callable=get_name)
    task2 = PythonOperator(task_id="get_age", python_callable=get_age)
    task3 = PythonOperator(task_id="greet", python_callable=task_greet)

    task1 >> task3
    task2 >> task3
