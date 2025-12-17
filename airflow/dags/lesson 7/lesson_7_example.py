from datetime import datetime, timedelta
import logging
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


def extract_data(**kwargs):
    logging.info("Extract task started")

    records_count = random.choice([0, 5, 10])

    logging.debug(f"Random choice result: {records_count}")
    logging.info(f"Records extracted: {records_count}")

    if records_count == 0:
        logging.warning("Extract returned zero records")

    return records_count


def validate_data(**kwargs):
    logging.info("Validation task started")

    ti = kwargs["ti"]
    records_count = ti.xcom_pull(task_ids="extract_data")

    logging.debug(f"XCom raw value: {records_count}")
    logging.info(f"Records received: {records_count}")

    if records_count == 0:
        logging.error("No records found. Data is invalid")
        raise AirflowFailException("Validation failed: empty dataset")

    logging.info("Data validation passed")


def load_data(**kwargs):
    logging.info("Load task started")

    logging.debug("Preparing data for load")
    logging.info("Data successfully loaded")


with DAG(
    dag_id="lesson_7_example_with_log_levels",
    description="DAG to demonstrate different logging levels",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args = {
        "owner": "airflow",
        "retries": 2,
        "retry_delay": timedelta(seconds=5)
    },
    tags=["education", "logging"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract_task >> validate_task >> load_task