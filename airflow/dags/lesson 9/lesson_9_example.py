from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def process_sales(run_date: str, input_path: str, filename: str, run_type: str) -> None:
    print("=== PythonOperator received parameters ===")
    print("run_date:", run_date)
    print("input_path:", input_path)
    print("filename:", filename)
    print("run_type:", run_type)

    full_path = f"{input_path}/{filename}"
    print("full_path:", full_path)

    if run_type == "full":
        print("Running FULL load logic")
    else:
        print("Running INCREMENTAL logic")

def print_config() -> None:
    config = Variable.get("demo_config", deserialize_json=True)
    print("demo_config:", config)
    print("batch_size:", config["batch_size"])
    print("owner_team:", config["owner_team"])


with DAG(
    dag_id="lesson_9_variables_and_templates_demo",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "run_type": "full",
        "source": "students_demo",
    },
    tags=["lesson-9", "variables", "templates"],
) as dag:

    show_templates_and_variables = BashOperator(
        task_id="show_templates_and_variables",
        bash_command="""
        echo "ds={{ ds }}"
        echo "ds_nodash={{ ds_nodash }}"
        echo "ts={{ ts }}"
        echo "data_path={{ var.value.demo_data_path }}"
        echo "full_path={{ var.value.demo_data_path }}/{{ ds }}"
        """,
    )

    show_json_variable = PythonOperator(
        task_id="show_json_variable",
        python_callable=print_config,
    )

    show_params = BashOperator(
        task_id="show_params",
        bash_command="""
        echo "run_type={{ params.run_type }}"
        echo "source={{ params.source }}"
        """,
    )

    process_sales_task = PythonOperator(
        task_id="process_sales_python",
        python_callable=process_sales,
        op_kwargs={
            "run_date": "{{ ds }}",
            "input_path": "{{ var.value.demo_data_path }}",
            "filename": "sales_{{ ds_nodash }}.csv",
            "run_type": "{{ params.run_type }}",
        },
    )

    show_templates_and_variables >> show_json_variable >> show_params >> process_sales_task