from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="dag_edu01_bash_operator",
    default_args=default_args,
    description="데이터 엔지니어링 수업 첫번째 DAG",
    start_date=datetime(2024, 5, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    task1 = BashOperator(
        task_id="task_edu1_echo",
        bash_command="echo hello airflow"
    )
