from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging
import random

def log_execution(func):
    def wrapper(**kwargs):
        task = kwargs["task"]
        ti = kwargs['ti']

        logging.info(f"Task started: {task.task_id}")

        logging.debug(f"run_id: {ti.run_id}, try_number: {ti.try_number}")

        result = func(**kwargs)
        logging.info(f"Task finished: {task.task_id}")
        logging.debug(f"Result: {result}")
        return result
    return wrapper


@log_execution
def extract(**kwargs):
    rows = random.randint(1, 5)
    logging.info(f"Extracted {rows} rows")

    if rows == 0:
        logging.warning("Extracted zero rows")

    return rows


@log_execution
def validate(**kwargs):
    ti = kwargs['ti']
    payload = ti.xcom_pull(task_ids='extract_task')

    logging.info(f"Validating {payload} rows")

    if payload == 0:
        logging.error("No data to validate")
        raise AirflowFailException("No data to validate")
    
    if payload < 3:
        logging.warning("Less than 3 rows extracted")
    
    return payload

@log_execution
def enrich(**kwargs):
    ti = kwargs['ti']
    validated = ti.xcom_pull(task_ids='processing_group.validate_task')

    enriched_date = {
        "rows": validated,
        "quality": "good" if validated >=3 else "poor"
    }

    logging.info(f"Enriched data: {enriched_date}")

    return enriched_date


@log_execution
def load(**kwargs):
    ti = kwargs['ti']
    payload = ti.xcom_pull(task_ids='processing_group.enrich_task')

    logging.info(f"Loading data: {payload}")

    if payload["quality"] == "poor":
        logging.warning("Loading data with poor quality")

    logging.info("Data loaded successfully")

with DAG(
    dag_id='class_work_7',
    start_date=datetime(2025, 1, 1),
    schedule='*/2 * * * * ',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': 10,
    },
    tags=['class_work_lesson_7']
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract
    )

    with TaskGroup('processing_group') as processing_group:
        
        validate_task = PythonOperator(
            task_id='validate_task',
            python_callable=validate
        )

        enrich_task = PythonOperator(
            task_id='enrich_task',
            python_callable=enrich
        )

    validate_task >> enrich_task

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load
    )

    extract_task >> processing_group >> load_task