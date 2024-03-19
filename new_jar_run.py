import json
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator


@dag(
    dag_id="jar_run_new",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="sample JAR run DAG",
    schedule=timedelta(minutes=10),
    start_date=datetime(2024, 1, 1),
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
        ti.xcom_push(key="jar_full_path", value=json.loads((conn.extra_dejson.get("path")))["path"] + "/demobatch-0.0.1.jar")

    read_jar_loc = read_jar_location()

    bash_pull = BashOperator(
        task_id="run_bash_jar",
        bash_command='echo "bash pull demo" && '
                     'echo "execution JAR is {{ task_instance.xcom_pull(task_ids="read_jar_location", key="jar_full_path") }}" && '
                     '/opt/airflow/jars/jdk-17.0.10/bin/java -jar {{ task_instance.xcom_pull(task_ids="read_jar_location", key="jar_full_path") }}',
        do_xcom_push=False,
    )


    read_jar_loc >> bash_pull


operator()
