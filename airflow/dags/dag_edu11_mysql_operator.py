import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.empty import EmptyOperator

dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="MySql operator을 활용한 DAG",
    start_date=datetime(2024, 5, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
) as dag:
    task = MySqlOperator(
        task_id="get_tables",
        mysql_conn_id="mysql_de_dw",
        sql="""
            show tables;
        """,
    )

    # Extract
    tasks_extract = []
    for day in range(1, 29):
        date = datetime(2024, 4, day)
        table = f"sbtest_{date.strftime('%Y%m%d')}"
        sql = "SELECT COUNT(*) FROM {{ params.table }} WHERE k > 6000"
        task = MySqlOperator(
            task_id=f"extract_{table}",
            mysql_conn_id="mysql_de_dw",
            sql=sql,
            params={"table": table},
        )
        tasks_extract.append(task)

    # Transform
    def func_task_transform(ti: TaskInstance):
        task_ids = ti.task.get_direct_relative_ids(True)
        returns = ti.xcom_pull(task_ids)
        result = 0
        for ret in returns:
            result += ret[0][0]
        print(f"Sum: {result}")
        return result

    task_transform = PythonOperator(
        task_id="transform", python_callable=func_task_transform
    )

    # Load
    sql = "INSERT INTO processed VALUES (DEFAULT, {{ ti.xcom_pull(task_ids='transform') }}, DEFAULT)"
    task_load = MySqlOperator(
        task_id="load",
        mysql_conn_id="mysql_de_dw",
        sql=sql,
    )

    # Set graph
    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")
    task_start >> tasks_extract >> task_transform >> task_load >> task_end
