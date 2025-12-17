from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def extract_data(**kwargs):
    logging.info("Extract started")

    logging.info(f"task_id={kwargs['task'].task_id}")
    logging.info(f"run_id={kwargs['run_id']}")

    result = {"rows": 150}
    logging.info(f"Extract result: {result}")

    return result


def transform_data(**kwargs):
    logging.info("Transform started")

    logging.info(f"task_id={kwargs['task'].task_id}")
    logging.info(f"run_id={kwargs['run_id']}")

    ti = kwargs["ti"]

    # Ошибка будет видна сразу в логах
    payload = ti.xcom_pull(task_ids="extract_data")

    logging.info(f"Pulled payload from XCom: {payload}")

    if payload is None:
        logging.error("XCom payload is None. Check task_ids in xcom_pull.")
        raise ValueError("No data received from extract task")

    result = {"processed_rows": payload["rows"]}
    logging.info(f"Transform result: {result}")

    return result


def load_data(**kwargs):
    logging.info("Load started")

    logging.info(f"task_id={kwargs['task'].task_id}")
    logging.info(f"run_id={kwargs['run_id']}")

    ti = kwargs["ti"]
    result = ti.xcom_pull(task_ids="processing_group.transform_data")

    logging.info(f"Pulled result from XCom: {result}")

    if result is None:
        logging.error("Nothing to load. Transform step did not produce data.")
        raise ValueError("Load failed due to missing data")

    logging.info("Load completed successfully")


with DAG(
    dag_id="lesson_6_broken_dag_with_logs",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=10)
    },
    tags=["lesson_6", "logging", "debugging"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
    )

    with TaskGroup("processing_group") as processing_group:
        transform = PythonOperator(
            task_id="transform_data",
            python_callable=transform_data,
            provide_context=True,
        )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True,
    )

    extract >> processing_group >> load