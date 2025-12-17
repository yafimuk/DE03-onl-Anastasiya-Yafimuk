from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def my_decorator(func):
    def wrapper():
        print("До вызова функции")
        func()
        print("После вызова функции")
    return wrapper


# # Декорируем обычную функцию
@my_decorator
def say_hello():
    print("Привет!")

@my_decorator
def say_goodbye():
    print("Пока!")

with DAG(
    dag_id="decorator_demo_simple",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
):

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=say_hello,  # уже обёрнутая функция
    )

    say_goodbye = PythonOperator(
        task_id="goodbye_task",
        python_callable=say_goodbye,  # уже обёрнутая функция
    )