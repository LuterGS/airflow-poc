from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook


@dag(
    dag_id="jar_run",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="sample JAR run DAG",
    schedule=timedelta(minutes=10),
    start_date=datetime(2024, 3, 18),
    catchup=False,
    tags=["smartcity"]
)
def operator():
    @task(task_id="read_jar_location")
    def read_jar_location():
        conn = BaseHook.get_connection("jar_loc")
        print("===== start print =====")
        print(conn)
        print("===== end print =====")


    read_jar_loc = read_jar_location()

    @task(task_id="print_test")
    def print_test():
        print("test")

    print_t = print_test()


    read_jar_loc >> print_t


operator()
