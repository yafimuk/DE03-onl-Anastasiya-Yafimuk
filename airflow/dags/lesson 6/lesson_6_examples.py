from datetime import datetime, timedelta
import logging
import random

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup


def extract_data(**kwargs):
    ti = kwargs["ti"]
    ds = kwargs["ds"]
    run_id = kwargs["run_id"]

    logging.info(f"Extract task started for ds={ds}, run_id={run_id}")
    print(f"Extract task started for ds={ds}, run_id={run_id}")

    rows = random.randint(50, 200)
    payload = {
        "rows": rows,
        "ds": ds,
        "run_id": run_id,
        "extracted_at": datetime.utcnow().isoformat(),
    }

    logging.info(f"Extract result: {payload}")
    return payload  # -> XCom


def validate_data(**kwargs):
    ti = kwargs["ti"]

    extract_payload = ti.xcom_pull(task_ids="extract_data")
    rows = extract_payload["rows"]

    logging.info(f"Validating extracted rows: {rows}")

    if rows < 80:
        logging.error("Validation failed: too few rows")
        raise AirflowFailException("Too few rows after extraction")

    result = {"validated_rows": rows}
    logging.info(f"Validation result: {result}")

    return result  # -> XCom


def transform_data(threshold: int, **kwargs):
    ti = kwargs["ti"]
    task_id = kwargs["task"].task_id

    extract_payload = ti.xcom_pull(task_ids="extract_data")
    rows = extract_payload["rows"]

    logging.info(
        f"Task {task_id}: transforming data "
        f"with threshold={threshold}, rows={rows}"
    )

    processed_rows = rows - random.randint(0, 20)
    status = "passed" if processed_rows >= threshold else "failed"

    result = {
        "processed_rows": processed_rows,
        "threshold": threshold,
        "status": status,
    }

    logging.info(f"Transform result: {result}")

    if status == "failed":
        raise AirflowFailException(
            f"Transform failed: processed_rows={processed_rows} < threshold={threshold}"
        )

    return result  # -> XCom


def load_data(**kwargs):
    ti = kwargs["ti"]
    execution_date = kwargs["execution_date"]

    transform_result = ti.xcom_pull(
        task_ids="processing_group.transform_data"
    )

    logging.info(
        f"Loading data for execution_date={execution_date}, "
        f"transform_result={transform_result}"
    )

    logging.info("Load completed successfully")


with DAG(
    dag_id="lesson_6_example_dag",
    description="Lesson 6: TaskGroups + params + XCom + kwargs context",
    start_date=datetime(2025, 1, 1),
    schedule="*/1 * * * *",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["lesson_6", "taskgroup", "xcom", "context"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    with TaskGroup(group_id="processing_group") as processing_group:
        validate = PythonOperator(
            task_id="validate_data",
            python_callable=validate_data,
        )

        transform = PythonOperator(
            task_id="transform_data",
            python_callable=transform_data,
            op_kwargs={"threshold": 120},
        )

        validate >> transform

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> processing_group >> load >> end