from datetime import timedelta, datetime

from airflow import XComArg
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator


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
    def read_jar_location(ti=None):
        conn = BaseHook.get_connection("jar_loc")
        print("===== start print =====")
        print(conn.extra_dejson.get("path"))
        print("===== end print =====")
        ti.xcom_push(key="jar_full_path", value=conn.extra_dejson.get("path") + "/demobatch-0.0.1.jar")

    read_jar_loc = read_jar_location()

    bash_pull = BashOperator(
        task_id="bash_pull",
        bash_command='echo "bash pull demo" && '
                     f'echo "The xcom pushed manually is {{ ti.xcom_pull(task_ids="read_jar_location", key="jar_full_path") }}"'
                     'echo "finished"',
        do_xcom_push=False,
    )


    read_jar_loc >> bash_pull


operator()
