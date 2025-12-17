from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime
import logging
import random

def log_execution(func):
    def wrapper(*args, **kwargs):
        logging.info(f"Starting task: {func.__name__}")
        result = func(*args, **kwargs)
        logging.info(f"Finished task: {func.__name__}")
        return result
    return wrapper

@log_execution
def generate_usd_rate(**kwargs):
    usd_rate = round(random.uniform(2.0, 7.0), 2)
    logging.info(f"Generated USD Rate: {usd_rate}")
    return usd_rate

@log_execution
def check_usd_rate(**kwargs):
    ti = kwargs['ti']
    usd_rate = ti.xcom_pull(task_ids='generate_task')
    if usd_rate < 3.0 or usd_rate > 6.0:
        raise AirflowFailException(f"USD rate {usd_rate} is out of acceptable range (3.0 - 6.0)")

@log_execution
def print_report(**kwargs):
    ti = kwargs['ti']
    usd_rate = ti.xcom_pull(task_ids='generate_task')
    logging.info(f"Final USD Rate Report: {usd_rate}")

with DAG(
    dag_id='class_work_6',
    start_date=datetime(2025, 1, 1),
    schedule='*/2 * * * * ',
    catchup=False,
    default_args={
        'retries': 2,
        'retry_delay': 10,
    },
    tags=['class_work_lesson_6']
) as dag:
    
    start_task = EmptyOperator(
        task_id='start_task'
    )

    generate_task = PythonOperator(
        task_id='generate_task',
        python_callable=generate_usd_rate
    )

    check_usd_rate_task = PythonOperator(
        task_id='check_usd_rate_task',
        python_callable=check_usd_rate
    )

    print_report_task = PythonOperator(
        task_id='print_report_task',
        python_callable=print_report
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )
    
    start_task >> generate_task >> check_usd_rate_task >> print_report_task >> end_task