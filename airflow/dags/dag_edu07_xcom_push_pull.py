import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator


def get_name(ti: TaskInstance):
    ti.xcom_push(key="first_name", value="Hyeonmin")
    ti.xcom_push(key="last_name", value="Lee")


def get_age(ti: TaskInstance):
    ti.xcom_push(key="age", value=27)


def task_greet(ti: TaskInstance):
    first_name = ti.xcom_pull(task_ids="get_name", key="first_name")
    last_name = ti.xcom_pull(task_ids="get_name", key="last_name")
    name = first_name + " " + last_name
    age = ti.xcom_pull(task_ids="get_age", key="age")
    print(f"Hello World! My name is {name} and I am {age} years old.")


dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="xcom의 push와 pull을 활용한 task간의 데이터 전달",
    start_date=datetime(2024, 5, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = PythonOperator(task_id="get_name", python_callable=get_name)
    task2 = PythonOperator(task_id="get_age", python_callable=get_age)
    task3 = PythonOperator(task_id="greet", python_callable=task_greet)

    [task1, task2] >> task3
