import psycopg2
import pandas as pd

conn = psycopg2.connect(
    dbname='dvd_rental_test',
    user='postgres',
    password='123',
    host='localhost',
    port='5432'
)

df = pd.read_sql_query('SELECT film_id, title FROM film WHERE release_year > 2000', conn)
print(df.head())

df['release_year'] = 2025 - df['film_id'] # Пример вычисляемого столбца
print(df.head())

films_start_from_a = df[df['title'].str.startswith('A')]
print(films_start_from_a)

conn.close()