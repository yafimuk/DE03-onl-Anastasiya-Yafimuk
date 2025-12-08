from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def show_full_info(country, name, age):
    print(f"{name}, {age} лет, страна: {country}")


with DAG(
    dag_id="params_op_args_kwargs_example",
    start_date=datetime(2024, 1, 1),
    schedule=None,
):

    task = PythonOperator(
        task_id="example_args_kwargs",
        python_callable=show_full_info,
        op_args=["Belarus"],                     # позиционный аргумент
        op_kwargs={"name": "Anastasiya", "age": 28},  # именованные аргументы
    )