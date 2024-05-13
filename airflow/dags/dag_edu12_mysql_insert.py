import os
import string
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.empty import EmptyOperator

dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5,
                "retry_delay": timedelta(minutes=2)}


def get_random_name(size: int = 10):
    return ''.join(random.choice(string.ascii_uppercase + string.digits)
                   for _ in range(size))


def func_generate_records(ti: TaskInstance):
    ea = random.randint(1, 5)
    records = [{
        "name": get_random_name(random.randint(10, 240)),
        "price": random.randint(100, 100000) * 100
    } for _ in range(ea)]
    ti.xcom_push(key="records", value=records)


def func_list_to_string(ti: TaskInstance):
    records = ti.xcom_pull(task_ids="extract", key="records")
    records_str = ", ".join(
        list(map(lambda x: str((x["name"], x["price"],)), records)))
    ti.xcom_push(key="records_str", value=records_str)


with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="MySql operator을 활용한 DAG",
    start_date=datetime(2024, 5, 1),
    schedule="1 * * * *",
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="extract",
        python_callable=func_generate_records
    )
    task2 = PythonOperator(
        task_id="transform",
        python_callable=func_list_to_string
    )
    task3 = MySqlOperator(
        task_id="load",
        mysql_conn_id="mysql_de_dw",
        sql="./sqls/insert_records_products.sql")

    task1 >> task2 >> task3
