from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


def process_users():
    # Наш список пользователей
    users = [
        {"name": "Alex", "age": 21, "active": True},
        {"name": "Maria", "age": 17, "active": False},
        {"name": "John", "age": 30, "active": True},
        {"name": "Anna", "age": 25, "active": False},
        {"name": "Tom",  "age": 16, "active": True}
    ]

    # 1. Количество активных пользователей
    active_users_count = 0
    for user in users:
        if user["active"] is True:
            active_users_count += 1

    # 2. Список имён пользователей 18+
    adult_user_names = []
    for user in users:
        if user["age"] >= 18:
            adult_user_names.append(user["name"])

    # 3. Список активных взрослых пользователей
    active_adult_users = []
    for user in users:
        if user["active"] and user["age"] >= 18:
            active_adult_users.append(user)

    # 4. Средний возраст всех пользователей
    total_age = 0
    for user in users:
        total_age += user["age"]

    average_age = total_age / len(users)

    # 5. Сводная информация
    summary = {
        "total_users": len(users),
        "active_users": active_users_count,
        "adult_users": len(adult_user_names),
        "average_age": average_age,
    }

    # 6. Проверка наличия несовершеннолетних
    has_minors = False
    for user in users:
        if user["age"] < 18:
            has_minors = True
            break

    if has_minors:
        message = "Есть несовершеннолетние пользователи"
    else:
        message = "Все пользователи взрослые"

    # Логи для Airflow
    print("Активных пользователей:", active_users_count)
    print("Взрослые пользователи:", adult_user_names)
    print("Активные взрослые:", active_adult_users)
    print("Средний возраст:", average_age)
    print("Сводная информация:", summary)
    print(message)


with DAG(
    dag_id="users_stats_dag",          # можешь переименовать под студента
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,            # только ручной запуск
    catchup=False,
    description="DAG для расчёта статистики по пользователям",
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    process_users_task = PythonOperator(
        task_id="process_users",
        python_callable=process_users,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> process_users_task >> end