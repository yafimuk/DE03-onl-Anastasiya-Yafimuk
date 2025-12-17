from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def log_execution(func):
    def wrapper(*args, **kwargs):
        print(f"Начинаю выполнение функции {func.__name__}")
        result = func(*args, **kwargs)
        print(f"Функция {func.__name__} завершена")
        return result
    return wrapper


@log_execution
def extract_function(number, multiplier, **kwargs):
    ti = kwargs["ti"]
    print("=== extract_task ===")
    print(f"task_id: {ti.task_id}")
    print(f"dag_id: {ti.dag_id}")
    print(f"execution_date: {ti.execution_date}")

    processed = number * multiplier
    print(f"Processed value: {processed}")
    ti.xcom_push(key="processed_value", value=processed)


def transform_function(name, **kwargs):
    ti = kwargs["ti"]
    processed = ti.xcom_pull(key="processed_value", task_ids="extract_task")

    print("=== transform_task ===")
    print(f"Pulled value from XCom: {processed}")

    message = f"Hello, {name}! Your processed number is {processed}"
    print(f"Message: {message}")
    ti.xcom_push(key="message", value=message)


def load_function(**kwargs):
    ti = kwargs["ti"]
    message = ti.xcom_pull(key="message", task_ids="transform_task")

    print("=== load_task ===")
    print(f"Final message: {message}")


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="classwork_lesson5_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
):

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_function,
        op_args=[10],                 # число
        op_kwargs={"multiplier": 3},  # множитель
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_function,
        op_kwargs={"name": "Anastasiya"},
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_function,
    )

    extract_task >> transform_task >> load_task