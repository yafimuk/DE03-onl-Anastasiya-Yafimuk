import pandas as pd

# 1. Extract
df = pd.read_csv('data.csv')

# 2. Transform
df['order_date'] = pd.to_datetime(df['order_date'])
df['total_amount'] = df['quantity'] * df['price']
df = df[df['order_date'] >= '2024-07-12']  # Фильтр по дате

print(df)