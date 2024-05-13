from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dag_edu02_multiple_tasks",
    default_args=default_args,
    description="Tasks 여러개 활용하는 DAG",
    start_date=datetime(2024, 5, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    task1 = BashOperator(task_id="task_edu2_first", bash_command="echo hello airflow")
    task2 = BashOperator(task_id="task_edu2_second", bash_command="echo airflow -ing")
    task3 = BashOperator(task_id="task_edu2_third", bash_command="echo bye airflow")

    task1.set_downstream(task2)
    task2 >> task3
