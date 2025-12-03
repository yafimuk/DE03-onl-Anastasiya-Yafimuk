from datetime import datetime
import os

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


DB_CONFIG = dict(
    dbname="postgres",
    user="postgres",
    password="123",
    host="host.docker.internal",  # важно: не 'localhost'
    port="5432",
)

# Путь к данным внутри контейнера Airflow
DATA_DIR = "/opt/airflow/dags/data"


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def create_tables():
    ddl = """
    -- BRONZE: сырые данные
    CREATE TABLE IF NOT EXISTS bronze_customers_raw (
        customer_id   INT,
        first_name    TEXT,
        last_name     TEXT,
        city          TEXT,
        signup_date   TEXT
    );

    CREATE TABLE IF NOT EXISTS bronze_orders_raw (
        order_id    INT,
        customer_id INT,
        order_date  TEXT,
        amount      NUMERIC(10,2)
    );

    -- SILVER: очищенные, нормализованные данные
    CREATE TABLE IF NOT EXISTS silver_customers_clean (
        customer_id   INT PRIMARY KEY,
        first_name    TEXT,
        last_name     TEXT,
        city          TEXT,
        signup_date   DATE
    );

    CREATE TABLE IF NOT EXISTS silver_orders_clean (
        order_id    INT PRIMARY KEY,
        customer_id INT,
        order_date  DATE,
        amount      NUMERIC(10,2)
    );

    -- GOLD: витрина для аналитики
    CREATE TABLE IF NOT EXISTS gold_customer_orders_mart (
        customer_id      INT PRIMARY KEY,
        full_name        TEXT,
        city             TEXT,
        signup_date      DATE,
        total_orders     INT,
        total_amount     NUMERIC(12,2),
        avg_order_amount NUMERIC(12,2),
        first_order_date DATE,
        last_order_date  DATE
    );
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    print("All tables created (if not existed).")


def bronze_load_customers():
    """
    Bronze: читаем customers.csv в pandas и грузим как есть в bronze_customers_raw.
    """
    file_path = os.path.join(DATA_DIR, "customers.csv")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"customers.csv not found at {file_path}")

    df = pd.read_csv(file_path)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE bronze_customers_raw;")

            rows = list(df.itertuples(index=False, name=None))
            cur.executemany(
                """
                INSERT INTO bronze_customers_raw (
                    customer_id, first_name, last_name, city, signup_date
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    print(f"Loaded {len(df)} rows into bronze_customers_raw.")


def bronze_load_orders():
    """
    Bronze: читаем orders.csv в pandas и грузим как есть в bronze_orders_raw.
    """
    file_path = os.path.join(DATA_DIR, "orders.csv")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"orders.csv not found at {file_path}")

    df = pd.read_csv(file_path)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE bronze_orders_raw;")

            rows = list(df.itertuples(index=False, name=None))
            cur.executemany(
                """
                INSERT INTO bronze_orders_raw (
                    order_id, customer_id, order_date, amount
                )
                VALUES (%s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    print(f"Loaded {len(df)} rows into bronze_orders_raw.")


# ---------- 3. SILVER: очистка и нормализация (pandas) ----------
def silver_build_customers():
    """
    Silver: забираем bronze_customers_raw в pandas, чистим и пишем в silver_customers_clean.

    Логика:
    - обрезаем пробелы у first_name/last_name/city
    - city: если пустой или только пробелы -> 'Unknown'
    - signup_date: парсим в дату
    - drop_duplicates по customer_id (оставляем последнюю запись)
    """
    with get_conn() as conn:
        df = pd.read_sql_query("SELECT * FROM bronze_customers_raw", conn)

    # trim строк
    for col in ["first_name", "last_name", "city"]:
        df[col] = df[col].astype(str).str.strip()

    # пустые city -> Unknown
    df["city"] = df["city"].replace(["", "nan", "None"], "Unknown")

    # signup_date в дату
    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date

    # дубликаты по customer_id – оставим последнюю запись
    df = df.sort_values("signup_date").drop_duplicates(
        subset=["customer_id"], keep="last"
    )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE silver_customers_clean;")

            rows = [
                (
                    int(row.customer_id),
                    row.first_name,
                    row.last_name,
                    row.city,
                    row.signup_date,
                )
                for row in df.itertuples(index=False)
            ]

            cur.executemany(
                """
                INSERT INTO silver_customers_clean (
                    customer_id, first_name, last_name, city, signup_date
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    print(f"Built silver_customers_clean: {len(df)} rows.")


def silver_build_orders():
    """
    Silver: забираем bronze_orders_raw и silver_customers_clean в pandas,
    чистим заказы и пишем в silver_orders_clean.

    Логика:
    - amount > 0
    - order_date -> DATE
    - берём только те заказы, у которых customer_id есть в silver_customers_clean
    - drop_duplicates по order_id (последняя запись)
    """
    with get_conn() as conn:
        df_orders = pd.read_sql_query("SELECT * FROM bronze_orders_raw", conn)
        df_customers = pd.read_sql_query(
            "SELECT customer_id FROM silver_customers_clean", conn
        )

    # Фильтр по amount > 0
    df_orders["amount"] = pd.to_numeric(df_orders["amount"], errors="coerce")
    df_orders = df_orders[df_orders["amount"] > 0]

    # Приводим order_date к дате
    df_orders["order_date"] = pd.to_datetime(df_orders["order_date"]).dt.date

    # Оставляем только заказы с существующими customer_id
    valid_ids = set(df_customers["customer_id"].tolist())
    df_orders = df_orders[df_orders["customer_id"].isin(valid_ids)]

    # Удаляем дубликаты по order_id (если вдруг есть)
    df_orders = df_orders.sort_values("order_date").drop_duplicates(
        subset=["order_id"], keep="last"
    )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE silver_orders_clean;")

            rows = [
                (
                    int(row.order_id),
                    int(row.customer_id),
                    row.order_date,
                    float(row.amount),
                )
                for row in df_orders.itertuples(index=False)
            ]

            cur.executemany(
                """
                INSERT INTO silver_orders_clean (
                    order_id, customer_id, order_date, amount
                )
                VALUES (%s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    print(f"Built silver_orders_clean: {len(df_orders)} rows.")


# ---------- 4. GOLD: витрина (агрегация в pandas) ----------

def gold_build_customer_orders_mart():
    """
    Gold: забираем silver_customers_clean и silver_orders_clean в pandas,
    делаем агрегаты и пишем в gold_customer_orders_mart.

    Агрегаты по customer_id:
    - full_name
    - city
    - signup_date
    - total_orders
    - total_amount
    - avg_order_amount
    - first_order_date
    - last_order_date
    """
    with get_conn() as conn:
        df_cust = pd.read_sql_query(
            "SELECT * FROM silver_customers_clean", conn
        )
        df_ord = pd.read_sql_query(
            "SELECT * FROM silver_orders_clean", conn
        )

    # join по customer_id
    df = df_ord.merge(df_cust, on="customer_id", suffixes=("_order", "_cust"))

    # агрегаты
    agg = (
        df.groupby(
            ["customer_id", "first_name", "last_name", "city", "signup_date"]
        )
        .agg(
            total_orders=("order_id", "count"),
            total_amount=("amount", "sum"),
            first_order_date=("order_date", "min"),
            last_order_date=("order_date", "max"),
        )
        .reset_index()
    )

    agg["avg_order_amount"] = agg["total_amount"] / agg["total_orders"]

    # формируем full_name
    agg["full_name"] = (agg["first_name"].fillna("") + " " +
                        agg["last_name"].fillna("")).str.strip()

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold_customer_orders_mart;")

            rows = [
                (
                    int(row.customer_id),
                    row.full_name,
                    row.city,
                    row.signup_date,
                    int(row.total_orders),
                    float(row.total_amount),
                    float(row.avg_order_amount),
                    row.first_order_date,
                    row.last_order_date,
                )
                for row in agg.itertuples(index=False)
            ]

            cur.executemany(
                """
                INSERT INTO gold_customer_orders_mart (
                    customer_id,
                    full_name,
                    city,
                    signup_date,
                    total_orders,
                    total_amount,
                    avg_order_amount,
                    first_order_date,
                    last_order_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
        conn.commit()
    print(f"Built gold_customer_orders_mart: {len(agg)} rows.")


# ---------- DAG ----------

with DAG(
    dag_id="postgres_pandas_bronze_silver_gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # ручной запуск
    catchup=False,
    description="Mini-ETL: CSV -> Bronze (Postgres) -> Silver (clean) -> Gold (mart) через pandas",
) as dag:

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    bronze_customers_task = PythonOperator(
        task_id="bronze_load_customers",
        python_callable=bronze_load_customers,
    )

    bronze_orders_task = PythonOperator(
        task_id="bronze_load_orders",
        python_callable=bronze_load_orders,
    )

    silver_customers_task = PythonOperator(
        task_id="silver_build_customers",
        python_callable=silver_build_customers,
    )

    silver_orders_task = PythonOperator(
        task_id="silver_build_orders",
        python_callable=silver_build_orders,
    )

    gold_mart_task = PythonOperator(
        task_id="gold_build_customer_orders_mart",
        python_callable=gold_build_customer_orders_mart,
    )

    # Зависимости слоёв
    create_tables_task >> [bronze_customers_task, bronze_orders_task]

    bronze_customers_task >> silver_customers_task
    [bronze_orders_task, silver_customers_task] >> silver_orders_task

    [silver_customers_task, silver_orders_task] >> gold_mart_task