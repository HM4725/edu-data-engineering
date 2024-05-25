import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

import MySQLdb.cursors

dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5,
                "retry_delay": timedelta(minutes=2)}


def get_batch_interval(**kwargs):
    date_begin = kwargs["dag_run"].data_interval_start
    date_end = kwargs["dag_run"].data_interval_end
    ti: TaskInstance = kwargs['ti']
    ti.xcom_push('date_begin', date_begin)
    ti.xcom_push('date_end', date_end)


def get_mysql_connection(mysql_conn_id):
    connection = MySqlHook.get_connection(mysql_conn_id)
    return {
        'user': connection.login,
        'password': connection.password,
        'host': connection.host,
        'database': connection.schema,
        'port': connection.port
    }


def generate_dump_command(mysql_conn_id, table_nm, ti: TaskInstance):
    conn_details = get_mysql_connection(mysql_conn_id)

    date_begin: datetime = ti.xcom_pull(
        task_ids='task_get_batch_interval', key="date_begin")
    date_end: datetime = ti.xcom_pull(
        task_ids='task_get_batch_interval', key="date_end")

    dump_command = f"""
    mysqldump -u {conn_details['user']} -p'{conn_details['password']}' \
        --host {conn_details['host']} --port {conn_details['port']} {conn_details['database']} {table_nm} \
        --where="created_at >= '{date_begin.strftime("%Y-%m-%d")}' AND created_at < '{date_end.strftime("%Y-%m-%d")}'" \
    > $AIRFLOW_HOME/dags/sqls/backup_{date_begin.strftime("%Y%m%d")}.sql
    """
    return dump_command


def generate_load_command(mysql_conn_id, ti: TaskInstance):
    conn_details = get_mysql_connection(mysql_conn_id)
    date_begin: datetime = ti.xcom_pull(
        task_ids='task_get_batch_interval', key="date_begin")
    load_command = f"""
    mysql -u {conn_details['user']} -p'{conn_details['password']}' \
        --host {conn_details['host']} --port {conn_details['port']} {conn_details['database']} \
    < $AIRFLOW_HOME/dags/sqls/backup_{date_begin.strftime("%Y%m%d")}.sql
    """
    return load_command


with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="MySql operator을 활용한 DAG",
    start_date=datetime(2024, 5, 15),
    schedule="0 0 * * *",
    catchup=False,
) as dag:

    table_nm = "products"

    task_get_interval = PythonOperator(
        task_id="task_get_batch_interval",
        python_callable=get_batch_interval
    )

    task_gen_dump = PythonOperator(
        task_id="task_generate_dump_command",
        python_callable=generate_dump_command,
        op_kwargs={"mysql_conn_id": "mysql_de_dw", "table_nm": table_nm}
    )

    task_gen_load = PythonOperator(
        task_id="task_generate_load_command",
        python_callable=generate_load_command,
        op_kwargs={"mysql_conn_id": "mysql_de_dw_sink"}
    )

    task_run_dump = BashOperator(
        task_id="task_run_mysqldump",
        bash_command="{{ ti.xcom_pull(task_ids='task_generate_dump_command') }}"
    )

    task_run_load = BashOperator(
        task_id="task_run_mysqlload",
        bash_command="{{ ti.xcom_pull(task_ids='task_generate_load_command') }}"
    )

    task_rename_table = MySqlOperator(
        task_id="task_change_name",
        mysql_conn_id="mysql_de_dw_sink",
        sql="""
        RENAME TABLE products TO
          products_{{ti.xcom_pull(task_ids='task_get_batch_interval', key="date_begin").strftime("%Y%m%d")}}
        """
    )

    task_start = EmptyOperator(task_id="start")

    task_end = EmptyOperator(task_id="end")

    task_start >> [task_get_interval, task_gen_load]
    task_get_interval >> task_gen_dump >> task_run_dump
    task_gen_load >> task_run_load
    task_run_dump >> task_run_load
    task_run_load >> task_rename_table >> task_end
