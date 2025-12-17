from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def read_grades(file_path: str, **kwargs):
    """
    Читает CSV и возвращает структуру:
    {
        "Alice": {"math": 8, "physics": 7, ...},
        ...
    }
    """
    df = pd.read_csv(file_path)
    # делаем student индексом и превращаем в dict
    df = df.set_index("student")
    data = df.to_dict(orient="index")

    print("=== read_grades ===")
    print(data)

    # возвращаем — автоматически попадёт в XCom (return_value)
    return data


def calculate_averages(**kwargs):
    """
    Берёт оценки из XCom и считает средний балл для каждого студента.
    Возвращает словарь {student: avg_score}.
    """
    ti = kwargs["ti"]
    grades = ti.xcom_pull(task_ids="read_grades_task")  # dict {student: {subject: score}}

    # Превращаем словарь в DataFrame: строки — студенты, столбцы — предметы
    df = pd.DataFrame.from_dict(grades, orient="index")

    # Среднее по строкам (по предметам одного студента)
    averages = df.mean(axis=1).to_dict()

    print("=== calculate_averages ===")
    print(averages)

    return averages  # уйдёт в XCom


def check_threshold(threshold: float, **kwargs):
    """
    Получает средние баллы и возвращает статусы:
    {student: 'passed' или 'failed'}.
    """
    ti = kwargs["ti"]
    averages = ti.xcom_pull(task_ids="processing_group.calculate_averages")

    result = {}
    for student, avg_score in averages.items():
        status = "passed" if avg_score >= threshold else "failed"
        result[student] = status

    print("=== check_threshold ===")
    print(f"threshold={threshold}")
    print(result)

    return result  # тоже уйдёт в XCom


def final_report(group_name: str, **kwargs):
    """
    Финальная задача: выводит исходные оценки, средние баллы и статусы.
    """
    ti = kwargs["ti"]

    grades = ti.xcom_pull(task_ids="read_grades_task")
    averages = ti.xcom_pull(task_ids="processing_group.calculate_averages")
    statuses = ti.xcom_pull(task_ids="processing_group.check_threshold")

    print("=== final_report ===")
    print(f"Group: {group_name}\n")

    for student, subjects in grades.items():
        subjects_str = ", ".join(
            f"{subj}={score}" for subj, score in subjects.items()
        )
        avg = averages[student]
        status = statuses[student]
        print(f"{student}: {subjects_str} | avg={avg:.2f} | {status}")


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="students_grades_taskgroup_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
):

    # 1. Чтение файла
    read_grades_task = PythonOperator(
        task_id="read_grades_task",
        python_callable=read_grades,
        op_kwargs={"file_path": "dags/data/students.csv"},
    )

    # 2. Группа обработки данных
    with TaskGroup(group_id="processing_group") as processing_group:
        calculate_averages_task = PythonOperator(
            task_id="calculate_averages",
            python_callable=calculate_averages,
        )

        check_threshold_task = PythonOperator(
            task_id="check_threshold",
            python_callable=check_threshold,
            op_kwargs={"threshold": 7},  # пример порога
        )

        calculate_averages_task >> check_threshold_task

    # 3. Финальный отчёт
    final_report_task = PythonOperator(
        task_id="final_report",
        python_callable=final_report,
        op_kwargs={"group_name": "Group A"},
    )

    # Общая последовательность: чтение файла → TaskGroup → финальный отчёт
    read_grades_task >> processing_group >> final_report_task