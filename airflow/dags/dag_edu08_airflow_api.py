import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task


dag_id = os.path.splitext(os.path.basename(__file__))[0]
default_args = {"owner": "hm", "retries": 5, "retry_delay": timedelta(minutes=2)}


@dag(
    dag_id=dag_id,
    default_args=default_args,
    description="Airflow API를 활용한 DAG [권장사항]",
    start_date=datetime(2024, 5, 1),
    schedule_interval="@daily",
    catchup=False,
)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {"first_name": "Hyeonmin", "last_name": "Lee"}

    @task()
    def get_age():
        return 27

    @task()
    def greet(first_name, last_name, age):
        name = first_name + " " + last_name
        print(f"Hello World! My name is {name} and I am {age} years old.")

    name_dict = get_name()
    age = get_age()
    greet(name_dict["first_name"], name_dict["last_name"], age)


dag_greet = hello_world_etl()
