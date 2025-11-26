import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="123",
    host="localhost", 
    port="5432"
)

cur = conn.cursor()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
tables = cur.fetchall()
print("List of table:", tables)

cur.close()
conn.close()