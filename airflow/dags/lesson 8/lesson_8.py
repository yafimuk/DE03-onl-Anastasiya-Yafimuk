from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

DAG_CONFIGS = [
    {"dag_id": "lesson_8_factory_alpha", "schedule": None, "name": "Alpha"},
    {"dag_id": "lesson_8_factory_beta",  "schedule": None, "name": "Beta"},
    {"dag_id": "lesson_8_factory_gamma", "schedule": None, "name": "Gamma"},
]


def make_dag(*, dag_id: str, schedule, name: str) -> DAG:
    def say_hello(**kwargs):
        print(f"Hello from {name}! dag_id={dag_id}")

    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule_interval=schedule,
        catchup=False,
        tags=["lesson", "factory"],
    ) as dag:
        PythonOperator(
            task_id="hello",
            python_callable=say_hello,
        )

    return dag


# Создаём несколько DAG-ов и регистрируем их в globals()
for cfg in DAG_CONFIGS:
    dag = make_dag(**cfg)
    globals()[cfg["dag_id"]] = dag