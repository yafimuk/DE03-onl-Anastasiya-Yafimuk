from datetime import datetime, timedelta
import logging
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


def _get_conf(**kwargs) -> dict:
    
    dag_run = kwargs.get("dag_run")
    conf = dag_run.conf or {}

    if not conf:
        raise AirflowFailException('dag_run.conf is required')

    if "allowed_counts" not in conf or "min_records" not in conf:
        raise AirflowFailException('dag_run.conf must contain "allowed_counts" and "min_records"')

    return conf


def extract_data(**kwargs):
    logging.info("Extract started")

    conf = _get_conf(**kwargs)
    allowed_counts = conf["allowed_counts"]

    logging.info(f"allowed_counts (dag_run.conf): {allowed_counts}")

    records = random.choice(allowed_counts)
    logging.info(f"Extracted records: {records}")

    return records


def validate_data(**kwargs):
    logging.info("Validation started")

    conf = _get_conf(**kwargs)
    min_records = conf["min_records"]

    records = kwargs["ti"].xcom_pull(task_ids="extract_data")

    logging.info(f"records={records}, min_records={min_records}")

    if int(records) < int(min_records):
        raise AirflowFailException("Not enough records")

    logging.info("Validation passed")


with DAG(
    dag_id="lesson_8_conf_only",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    tags=["lesson", "dag_run_conf"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    extract >> validate