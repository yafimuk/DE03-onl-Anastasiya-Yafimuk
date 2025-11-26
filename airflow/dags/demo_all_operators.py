from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.email import EmailOperator  # core


def say_hello():
    print("Hello from PythonOperator!")


def choose_branch(**context):
    minute = datetime.utcnow().minute
    print(f"Current UTC minute: {minute}")
    return "even_branch" if minute % 2 == 0 else "odd_branch"


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="demo_all_operators_full",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Демонстрационный DAG с примерами основных операторов (core)",
):

    start = EmptyOperator(task_id="start")

    bash_task = BashOperator(
        task_id="bash_example",
        bash_command="echo 'Hello from BashOperator!' && ls -l",
    )

    python_task = PythonOperator(
        task_id="python_example",
        python_callable=say_hello,
    )

    sensor_task = TimeDeltaSensor(
        task_id="sensor_wait_5_seconds",
        delta=timedelta(seconds=5),
        poke_interval=1,
        mode="reschedule",
    )

    email_task = EmailOperator(
        task_id="email_example",
        to="nastafirik@gmail.com",
        subject="Test Email from Airflow",
        html_content="<h3>Hello from EmailOperator</h3>",
    )

    branch = BranchPythonOperator(
        task_id="branch_example",
        python_callable=choose_branch,
    )

    even_branch = BashOperator(
        task_id="even_branch",
        bash_command="echo 'Even branch executed!'",
    )

    odd_branch = BashOperator(
        task_id="odd_branch",
        bash_command="echo 'Odd branch executed!'",
    )

    end = EmptyOperator(task_id="end")

    start >> bash_task >> python_task >> sensor_task >> branch
    branch >> [even_branch, odd_branch] >> end