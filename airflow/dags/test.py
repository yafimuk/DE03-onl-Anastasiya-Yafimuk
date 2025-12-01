from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import pandas as pd
def merge_files_csv():
    df1= pd.read_csv ('/opt/airflow/dags/data1.csv')
    df2= pd.read_csv('/opt/airflow/dags/data2.csv')
    merge = pd.concat([df1,df2], ignore_index=True)
    merge=merge.drop_duplicates(subset="id", keep="last")
                      
    merge.to_csv ('/opt/airflow/dags/merge.csv',index=False)

with DAG(
    dag_id= "Hanna_merge_json_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval = None,
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id= "start")

    python_task = PythonOperator(
        task_id = "merge_csv",
        python_callable = merge_files_csv
    )
    end= EmptyOperator(task_id="end")

    start >> python_task >> end