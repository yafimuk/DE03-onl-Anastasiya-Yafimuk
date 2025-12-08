from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def show_info(name, age):
    print(f"Имя: {name}, возраст: {age}")


with DAG(
    dag_id="params_op_args_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
):

    task = PythonOperator(
        task_id="example_args",
        python_callable=show_info,
        op_args=["Alice", 25],  # ← Передача по позиции
    )