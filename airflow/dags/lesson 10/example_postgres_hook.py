from datetime import datetime
import psycopg2
from airflow import DAG
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook


class SimplePostgresHook(BaseHook):

    def __init__(self, conn_id="my_postgres"):
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self):
        conn = self.get_connection(self.conn_id)

        # Build psycopg2 connection
        pg_conn = psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            database=conn.schema or "postgres",
        )

        return pg_conn

    def run_query(self, sql: str):
        conn = self.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result


class PostgresQueryOperator(BaseOperator):
    def __init__(self, sql: str, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self._hook = SimplePostgresHook()

    def execute(self, context):
        self.log.info("Running PostgreSQL query: %s", self.sql)
        result = self._hook.run_query(self.sql)
        self.log.info("Query returned: %s", result)
        return result


with DAG(
    dag_id="example_postgres_hook",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
):

    query_task = PostgresQueryOperator(
        task_id="select_from_pg",
        sql="SELECT datname FROM pg_database;",
    )