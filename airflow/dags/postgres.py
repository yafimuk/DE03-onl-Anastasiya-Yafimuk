from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2


def list_tables():
    # Подключаемся к ЛОКАЛЬНОМУ Postgres на Mac из контейнера Airflow
    conn = psycopg2.connect(
        dbname="postgres",      # как в твоём локальном скрипте
        user="postgres",
        password="123",
        host="host.docker.internal",  # ВАЖНО: не 'localhost'!
        port="5432",
    )

    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
    )
    tables = cur.fetchall()

    # Это увидишь в логах таски
    print("List of tables in public schema:")
    for row in tables:
        print(f"- {row[0]}")

    cur.close()
    conn.close()


with DAG(
    dag_id="postgres_list_tables",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # только ручной запуск
    catchup=False,
    description="Пример DAG, который ходит в локальный Postgres и выводит список таблиц",
) as dag:

    list_tables_task = PythonOperator(
        task_id="list_tables",
        python_callable=list_tables,
    )