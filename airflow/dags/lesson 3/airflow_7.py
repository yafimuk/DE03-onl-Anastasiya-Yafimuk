from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def my_decorator(func):
    def wrapper(**kwargs):
        print('Функция началась')
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        print('Завершена')
    return wrapper    

def extract(number, multiplier, **kwargs):
    ti = kwargs['ti']
    dag_id = kwargs['ti']
    execution_date = kwargs['exectuion_date']
    calculate_value = number * multiplier
    ti.xcom_push(key='processed_value', value = calculate_value)
    print(f"task_id: {ti.task_id}, dag_id = {ti.dag_id}, execution_date = {ti.execution_date}" )
    return calculate_value
   

@my_decorator
def transform(name, **kwargs):
    ti = kwargs['ti']
    calculate_value = ti.xcom_pull(key='processed_value', task_ids = 'extract_task')
    message = f"Hello! {name}, Your processed number is {calculate_value}"
    ti.xcom_push(key="message", value = message)

def load(**kwargs):
    ti = kwargs['ti']
    ti.xcom_pull(key="message", task_ids = 'transform_task')

default_args = {
    "owner" : "user",
    "start_date": datetime(2024, 1, 1),
}

with DAG (
    dag_id = "student_practice_dag",
    default_args = default_args,
    schedule = None,
    catchup = False,
    tags = ["ETL"],
):
    extract_task = PythonOperator(
        task_id = 'extract_task',
        python_callable = extract,
        op_args = [10],
        op_kwargs = {'multiplier': 5}
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
        op_kwargs={'name': 'Maksim'}
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load,
    )

    extract_task >> transform_task >> load_task