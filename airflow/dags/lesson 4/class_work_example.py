from datetime import datetime, timedelta
import random

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


def multiply_result(factor: int):
    """
    Декоратор: умножает числовой результат функции на factor.
    Если результат не число — возвращает как есть.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            if isinstance(result, (int, float)):
                return result * factor
            return result
        return wrapper
    return decorator


def generate_numbers(count: int, min_value: int, max_value: int, **kwargs):
    """
    Генерирует список случайных чисел и возвращает его.
    Возвращаемое значение автоматически попадёт в XCom (ключ return_value).
    """
    numbers = [random.randint(min_value, max_value) for _ in range(count)]
    print("=== generate_numbers ===")
    print(f"Generated numbers ({len(numbers)}): {numbers}")
    return numbers


def calculate_statistics(**kwargs):
    """
    Берём список чисел из XCom (из задачи generate_numbers_task),
    считаем min, max и среднее, возвращаем как словарь.
    """
    ti = kwargs["ti"]
    numbers = ti.xcom_pull(task_ids="generate_numbers_task")

    print("=== calculate_statistics ===")
    print(f"Source numbers: {numbers}")

    stats = {
        "min": min(numbers),
        "max": max(numbers),
        "avg": sum(numbers) / len(numbers),
    }
    print(f"Stats: {stats}")
    return stats


@multiply_result(factor=10)
def calculate_score(threshold: float, **kwargs):
    """
    Считает “оценку” на основе среднего значения.
    Базовая логика:
      - если avg >= threshold → score = 1
      - иначе score = 0
    Декоратор multiply_result умножит результат на 10.
    """
    ti = kwargs["ti"]
    stats = ti.xcom_pull(
        task_ids="processing_group.calculate_statistics"
    )
    avg_value = stats["avg"]

    print("=== calculate_score ===")
    print(f"Average value: {avg_value}, threshold: {threshold}")

    base_score = 1 if avg_value >= threshold else 0
    print(f"Base score (before decorator): {base_score}")
    return base_score  # реально в XCom уйдёт base_score * 10


def final_report(student_name: str, **kwargs):
    """
    Финальный таск: забирает данные из всех задач и печатает отчёт.
    """
    ti = kwargs["ti"]

    numbers = ti.xcom_pull(task_ids="generate_numbers_task")
    stats = ti.xcom_pull(task_ids="processing_group.calculate_statistics")
    score = ti.xcom_pull(task_ids="processing_group.calculate_score")

    print("=== final_report ===")
    print(f"Student: {student_name}")
    print(f"Numbers: {numbers}")
    print(f"Min: {stats['min']}, Max: {stats['max']}, Avg: {stats['avg']}")
    print(f"Final score (after decorator): {score}")


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="student_taskgroups_with_decorators_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
):

    # 1) Генерация чисел
    generate_numbers_task = PythonOperator(
        task_id="generate_numbers_task",
        python_callable=generate_numbers,
        op_kwargs={
            "count": 10,
            "min_value": 1,
            "max_value": 100,
        },
    )

    # 2) Группа для обработки чисел
    with TaskGroup(group_id="processing_group") as processing_group:
        calculate_statistics_task = PythonOperator(
            task_id="calculate_statistics",
            python_callable=calculate_statistics,
        )

        calculate_score_task = PythonOperator(
            task_id="calculate_score",
            python_callable=calculate_score,
            op_kwargs={"threshold": 50},  # переброс параметра в функцию
        )

        calculate_statistics_task >> calculate_score_task

    # 3) Финальный отчёт
    final_report_task = PythonOperator(
        task_id="final_report",
        python_callable=final_report,
        op_kwargs={"student_name": "Anastasiya"},
    )

    # Общая цепочка
    generate_numbers_task >> processing_group >> final_report_task