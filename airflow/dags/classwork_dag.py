from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.email import EmailOperator

STUDENT_NAME = "Nastya"          # ваше имя (латиницей, для dag_id)
BIRTH_YEAR = 1997                # ваш год рождения

def calc_age(**kwargs):
    current_year = datetime.now().year
    age = current_year - BIRTH_YEAR
    print(f"Current year: {current_year}")
    print(f"Birth year: {BIRTH_YEAR}")
    print(f"Your age is: {age}")
    return age 


def choose_branch(**kwargs):
    ti = kwargs["ti"]
    age = ti.xcom_pull(task_ids="python_age")
    print(f"Age pulled from XCom: {age}")
    if age % 2 == 0:
        return "even_task"
    else:
        return "odd_task"


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id=f"{STUDENT_NAME.lower()}_first_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # только ручной запуск
    catchup=False,
    default_args=default_args,
    description="Практическое задание: первый DAG с ветвлением по возрасту",
):
    # 1. Стартовая задача
    start = EmptyOperator(task_id="start")

    # 2. BashOperator — вывод имени
    bash_name = BashOperator(
        task_id="bash_name",
        bash_command=f'echo "Hi, my name is {STUDENT_NAME}"',
    )

    # 3. PythonOperator — вычисление возраста
    python_age = PythonOperator(
        task_id="python_age",
        python_callable=calc_age,
    )

    # 4. Sensor — ожидание 5 секунд
    wait_5_sec = TimeDeltaSensor(
        task_id="wait_5_sec",
        delta=timedelta(seconds=5),
        poke_interval=1,
        mode="reschedule",
    )

    # 5. BranchPythonOperator — выбираем ветку по чётности возраста
    branch = BranchPythonOperator(
        task_id="branch_age_even_or_odd",
        python_callable=choose_branch,
    )

    # 6. Ветка для чётного возраста
    even_task = BashOperator(
        task_id="even_task",
        bash_command='echo "Your age is even!"',
    )

    # 7. Ветка для нечётного возраста
    odd_task = BashOperator(
        task_id="odd_task",
        bash_command='echo "Your age is odd!"',
    )

    # 8. Финальная задача
    end = EmptyOperator(task_id="end")

    start >> bash_name >> python_age >> wait_5_sec >> branch
    branch >> [even_task, odd_task] >> end