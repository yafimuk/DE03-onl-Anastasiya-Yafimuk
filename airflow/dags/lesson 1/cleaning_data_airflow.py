from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd


def clean_students_data():
    df = pd.read_csv('/opt/airflow/dags/lesson1/students.csv')

    df['Имя'] = df['Имя'].str.strip().str.capitalize()
    df['Город'] = df['Город'].str.strip().str.capitalize()

    df['Возраст'] = df['Возраст'].replace({'восемнадцать': '18'}).astype(int)

    def normalize_gender(x):
        if pd.isna(x):
            return None
        x = x.lower()
        if x in ['м', 'муж']:
            return 'М'
        if x in ['ж', 'жен']:
            return 'Ж'
        return x.capitalize()

    df['Пол'] = df['Пол'].apply(normalize_gender)

    df['Город'] = df['Город'].replace({
        'Minsk': 'Минск',
        'мИнск': 'Минск',
        'минск': 'Минск',
        'Gomel': 'Гомель'
    })

    df = df.drop_duplicates(subset=['Email', 'Баллы'])

    df['Баллы'] = df['Баллы'].fillna(0).astype(int)
    df['Email'] = df['Email'].fillna('no_email@unknown.com')
    df['Пол'] = df['Пол'].fillna('Не указан')

    df.to_csv('/opt/airflow/dags/students_cleaned.csv', index=False)
    print("Файл students_cleaned.csv успешно сохранён")


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="students_cleaning_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",  # можно None, если хочешь запускать только руками
    catchup=False,
    default_args=default_args,
    description="Автоматическая очистка файла students.csv",
) as dag:

    clean_task = PythonOperator(
        task_id="clean_students_data",
        python_callable=clean_students_data,
    )