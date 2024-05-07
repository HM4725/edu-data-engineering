# test.py

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="test_python_operator",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "edu-data-engineering"],
) as dag:

    def test():
        print("this is a test")

    run_this = PythonOperator(
        task_id="test",
        python_callable=test,
    )