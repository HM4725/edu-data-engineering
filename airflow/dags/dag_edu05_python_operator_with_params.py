import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def task_greet(name, age):
    print(f"Hello World! My name is {name} and I am {age} years old.")


dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="인자를 받는 Python operator를 활용하는 DAG",
    start_date=datetime(2024, 5, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="task1",
        python_callable=task_greet,
        op_kwargs={"name": "hm", "age": 27},
    )
