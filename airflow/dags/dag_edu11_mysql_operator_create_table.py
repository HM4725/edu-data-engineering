import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.empty import EmptyOperator

dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5,
                "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="MySql operator을 활용한 DAG",
    start_date=datetime(2024, 5, 1),
    schedule="0 0 * * *",
    catchup=False,
) as dag:
    task_create_source_table = MySqlOperator(
        task_id="create_source_table",
        mysql_conn_id="mysql_de_dw",
        sql="./sqls/create_tb_products.sql"
    )
    task_create_target_table = MySqlOperator(
        task_id="create_sink_table",
        mysql_conn_id="mysql_de_dw_sink",
        sql="./sqls/create_tb_products.sql"
    )

    # Set graph
    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")
    task_start >> task_create_source_table >> task_create_target_table >> task_end
