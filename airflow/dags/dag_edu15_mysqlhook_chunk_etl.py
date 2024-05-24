import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.empty import EmptyOperator

import MySQLdb.cursors

dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5,
                "retry_delay": timedelta(minutes=2)}


def func_print_boundary(**kwargs):
    date_begin = kwargs["dag_run"].data_interval_start
    date_end = kwargs["dag_run"].data_interval_end
    print(date_begin, date_end)


def func_etl(**kwargs):
    source_hook = MySqlHook(mysql_conn_id="mysql_de_dw")
    target_hook = MySqlHook(mysql_conn_id="mysql_de_dw_sink")

    source_conn = source_hook.get_conn()
    target_conn = target_hook.get_conn()

    # cursor: client-side store / sscursor: server-side store
    source_cursor = source_conn.cursor(MySQLdb.cursors.SSCursor)
    target_cursor = target_conn.cursor()

    date_begin = kwargs["dag_run"].data_interval_start
    date_end = kwargs["dag_run"].data_interval_end

    sql_extract = f"""
    SELECT *
    FROM products
    WHERE created_at >= '{date_begin.strftime("%Y-%m-%d")}'
    AND created_at < '{date_end.strftime("%Y-%m-%d")}';
    """

    sql_load = f"""
    INSERT INTO products_{date_begin.strftime("%Y%m%d")}
    VALUES (%s, %s, %s, %s, %s)
    """

    # etl per chunk_size
    source_cursor.execute(sql_extract)
    chunk_size = 1000
    while True:
        rows = source_cursor.fetchmany(chunk_size)
        if not rows:
            break
        target_cursor.executemany(
            sql_load,
            rows
        )
        target_conn.commit()

    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()


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

    task_etl = PythonOperator(
        task_id="etl",
        python_callable=func_etl
    )

    # Set graph
    task_start = EmptyOperator(task_id="start")
    task_end = EmptyOperator(task_id="end")
    task_start >> task_print_boundary >> task_create_table \
        >> task_etl >> task_end
