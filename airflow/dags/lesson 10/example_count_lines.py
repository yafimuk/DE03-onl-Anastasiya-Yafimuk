from datetime import datetime

from airflow import DAG
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook


class CountLinesOperator(BaseOperator):
    def __init__(self, file_path: str, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path

    def execute(self, context):
        self.log.info("Reading file directly in operator: %s", self.file_path)
        with open(self.file_path, "r") as f:
            lines = f.readlines()
        line_count = len(lines)
        self.log.info("Line count (direct): %s", line_count)
        return line_count


class LocalFileHook(BaseHook):
    def count_lines(self, file_path: str) -> int:
        with open(file_path, "r") as f:
            lines = f.readlines()
        return len(lines)


class CountLinesWithHookOperator(BaseOperator):
    def __init__(self, file_path: str, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self._hook = LocalFileHook()

    def execute(self, context):
        self.log.info("Reading file via LocalFileHook: %s", self.file_path)
        line_count = self._hook.count_lines(self.file_path)
        self.log.info("Line count (via hook): %s", line_count)
        return line_count


with DAG(
    dag_id="example_count_lines",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    count_direct = CountLinesOperator(
        task_id="count_file_lines_direct",
        file_path="/opt/airflow/dags/data/customers.csv",
    )

    count_via_hook = CountLinesWithHookOperator(
        task_id="count_file_lines_via_hook",
        file_path="/opt/airflow/dags/data/customers.csv",
    )

    count_direct >> count_via_hook