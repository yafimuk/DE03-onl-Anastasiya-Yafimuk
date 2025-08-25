import pandas as pd

df = pd.read_csv('python/lesson 9/data.csv')
print(df)   

reach_customer = df[df['price'] > 1000]
print(reach_customer)

sorted_df = df.sort_values(by='price', ascending=False)  # Сортирует DataFrame по столбцу 'final_price' в порядке убывания
print('Сортировка по price (по убыванию): ', sorted_df)

df['total_price'] = df['price'] * df['quantity']
print(df)

df = df.drop_duplicates(subset=['customer_name'])
df = df.fillna({'product': 'Unknown'})
print(df['product'])