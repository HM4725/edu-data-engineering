import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="catchup과 backfill을 활용한 DAG",
    start_date=datetime(2024, 5, 1),
    schedule="@daily",
    catchup=True,
) as dag:
    task1 = BashOperator(
        task_id="task1", bash_command="echo This is a simple bash command!"
    )

"""
Try in bash:
`airflow dags backfill -s 2024-05-04 -e 2024-05-09 dag_edu9_catchup_backfill`
"""
