from datetime import datetime, timedelta
import logging
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException


def extract_data(**kwargs):
    logging.info("Extract started")

    params = kwargs["params"]
    allowed_counts = params["allowed_counts"]

    logging.info(f"allowed_counts (params): {allowed_counts}")

    records = random.choice(allowed_counts)
    logging.info(f"Extracted records: {records}")

    return records


def validate_data(**kwargs):
    logging.info("Validation started")

    params = kwargs["params"]
    min_records = params["min_records"]

    ti = kwargs["ti"]
    records = ti.xcom_pull(task_ids="extract_data")

    logging.info(f"records={records}, min_records={min_records}")

    if int(records) < int(min_records):
        raise AirflowFailException("Not enough records")

    logging.info("Validation passed")


with DAG(
    dag_id="lesson_8_params_only",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={
        "allowed_counts": [0, 5, 10],
        "min_records": 1,
    },
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    tags=["lesson", "params"],
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