"""
[preset]   [cron]
None       -
@once      -
@hourly    0 * * * *
@daily     0 0 * * *
@weekly    0 0 * * 0
@monthly   0 0 1 * *
@yearly    0 0 1 1 *
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="Cron expression을 활용한 DAG",
    start_date=datetime(2024, 5, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    task1 = BashOperator(
        task_id="task1", bash_command="echo This is a simple bash command!"
    )

"""
Reference: https://crontab.guru/
"""
