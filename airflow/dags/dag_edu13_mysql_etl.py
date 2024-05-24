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


def func_print_boundary(**kwargs):
    date_begin = kwargs["dag_run"].data_interval_start
    date_end = kwargs["dag_run"].data_interval_end
    print(date_begin, date_end)


def func_transform(ti: TaskInstance):
    data: tuple[tuple[int, str, int, datetime, datetime]
                ] = ti.xcom_pull('extract')
    result = map(lambda x: str((x[0], x[1], x[2],
                                x[3].strftime('%Y-%m-%d %H:%M:%S'),
                                x[4].strftime('%Y-%m-%d %H:%M:%S'),)),
                 data)
    return ",".join(list(result))


with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="MySql operator을 활용한 DAG",
    start_date=datetime(2024, 5, 15),
    schedule="0 0 * * *",
    catchup=True,
) as dag:
    task_print_boundary = PythonOperator(
        task_id="print_boundary",
        python_callable=func_print_boundary,
    )

    task_create_table = MySqlOperator(
        task_id="create_partitioned_table",
        mysql_conn_id="mysql_de_dw_sink",
        sql="./sqls/create_tb_partitioned_products.sql"
    )

    task_extract = MySqlOperator(
        task_id="extract",
        mysql_conn_id="mysql_de_dw",
        sql="./sqls/read_records_products.sql"
    )

    task_transform = PythonOperator(
        task_id="transform",
        python_callable=func_transform
    )

    task_load = MySqlOperator(
        task_id="load",
        mysql_conn_id="mysql_de_dw_sink",
        sql="./sqls/insert_records_partitioned_products.sql"
    )

    # Set graph
    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")
    task_start >> task_print_boundary >> task_create_table \
        >> task_extract >> task_transform >> task_load >> task_end
