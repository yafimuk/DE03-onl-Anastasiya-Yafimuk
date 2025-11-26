from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta


def say_hello():
    print("Hello, Airflow! This is my first task.")


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="demo_basic_airflow",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    description="Минимальная демонстрация Airflow DAG",
) as dag:

    start = EmptyOperator(task_id="start")

    hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    end = EmptyOperator(task_id="end")

    start >> hello >> end