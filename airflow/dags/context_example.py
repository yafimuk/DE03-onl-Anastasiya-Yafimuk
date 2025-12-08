from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def my_task_function(**kwargs):
    print("Task context:")
    for key, value in kwargs.items():
        print(f"{key}: {value}")
    


with DAG(
    dag_id="context_example_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):

    show_context = PythonOperator(
        task_id="show_context",
        python_callable=my_task_function,
    )