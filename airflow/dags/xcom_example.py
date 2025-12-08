from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# --- Задача 1: кладём значение в XCom ---
def push_value(**context):
    ti = context["ti"]  # TaskInstance
    ti.xcom_push(key="number", value=42)
    print("Значение отправлено в XCom!")


# --- Задача 2: читаем значение из XCom ---
def pull_value(**context):
    ti = context["ti"]
    value = ti.xcom_pull(key="number", task_ids="push_task")
    print(f"Полученное из XCom значение: {value}")


with DAG(
    dag_id="simple_xcom_example_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):

    push_task = PythonOperator(
        task_id="push_task",
        python_callable=push_value,
    )

    pull_task = PythonOperator(
        task_id="pull_task",
        python_callable=pull_value,
    )

    push_task >> pull_task