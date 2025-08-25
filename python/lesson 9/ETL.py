import pandas as pd
import psycopg2

df = pd.read_csv("get_data\data.csv")


df["order_date"] = pd.to_datetime(df["order_date"])
df["total_amount"] = df["quantity"] * df["price"]
df = df[df["order_date"] >= "2024-07-12"]  

conn = psycopg2.connect(
    dbname="postgres", 
    user="postgres", 
    password="123", 
    host="localhost", 
    port="5432"
)
cur = conn.cursor()

for _, row in df.iterrows():
    cur.execute(
        """
        INSERT INTO orders (order_id, customer_name, order_date, product, quantity, price, total_amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """,
        (
            row["order_id"],
            row["customer_name"],
            row["order_date"],
            row["product"],
            row["quantity"],
            row["price"],
            row["total_amount"],
        ),
    )

conn.commit()
cur.close()
conn.close()
