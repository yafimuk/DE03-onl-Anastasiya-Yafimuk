from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def extract_data(**kwargs):
    logging.info("Extract started")
    return {"rows": 150}


def transform_data(**kwargs):
    ti = kwargs["ti"]
    payload = ti.xcom_pull(task_ids="extract")
    logging.info(f"Pulled payload: {payload}")
    return {"processed_rows": payload["rows"]}


def load_data(**kwargs):
    ti = kwargs["ti"]
    result = ti.xcom_pull(task_ids="process_group.transform_data")
    logging.info(f"Loading: {result}")


with DAG(
    dag_id="lesson_6_broken_dag",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=10)},
    tags=["lesson_6", "broken"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    with TaskGroup("processing_group") as processing_group:
        transform = PythonOperator(
            task_id="transform_data",
            python_callable=transform_data,
        )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract >> processing_group >> load