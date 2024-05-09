from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dag_edu3_multiple_tasks",
    default_args=default_args,
    description="Tasks 여러개 활용하는 DAG",
    start_date=datetime(2024, 5, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    task1 = BashOperator(task_id="task_edu2_first", bash_command="echo hello airflow")
    task2 = BashOperator(task_id="task_edu2_second1", bash_command="echo bye airflow1")
    task3 = BashOperator(task_id="task_edu2_second2", bash_command="echo bye airflow2")

    task1 >> task2
    task1 >> task3
