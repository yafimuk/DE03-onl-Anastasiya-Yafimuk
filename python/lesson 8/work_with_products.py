import pandas as pd
import numpy as np

df = pd.read_csv(r'python/lesson 8/products.csv')

print(df) # Выводит содержимое DataFrame
print(df.head()) # Показывает первые 5 строк
print(df.tail()) # Показывает последние 5 строк
print(df.shape) # Показывает размерность DataFrame
print(df.columns) # Показывает названия столбцов
print(df.info()) # Показывает информацию о DataFrame
print(df.describe()) # Показывает статистические характеристики числовых столбцов

print(df['product']) # Выводит столбец 'Product'
print(df[['product', 'category']]) # Выводит столбцы 'Product' и 'Price'
print(df.iloc[0]) # Выводит первую строку DataFrame
print(df.iloc[0:3]) # Выводит первые три строки DataFrame
print(df.loc[0]) # Выводит первую строку DataFrame по метке индекса
print(df.loc[0:2]) # Выводит строки с 0 по 2 по метке индекса

prices = [ 10.99, 20.50, 15.75, 30.00, 25.00, 18.25, 22.10]
df['price'] = prices  # Добавляет новый столбец 'price' с указанными значениями
print(df)
df['price'] = np.random.randint(1000, 5000, size=len(df))  # Добавляет новый столбец 'price' с случайными значениями
df['discount'] = 0.15  # Добавляет новый столбец 'discount' со значением 0.1
df['final_price'] = df['price'] * (1 - df['discount']).round(2)  # Вычисляет финальную цену с учетом скидки

max_price = df.groupby('category')['final_price'].max()  # Находит максимальную цену в каждой категории
print('Максимальное значение: ', max_price)

sum_price = df.groupby('category')['final_price'].sum()  # Находит сумму цен в каждой категории
print('Сумма по категории: ', sum_price)

mean_price = df.groupby('category')['final_price'].mean()  # Находит среднюю цену в каждой категории
print('Среднее значение price:', mean_price)

expencive = df[df['final_price'] > 3000]  # Фильтрует товары с финальной ценой больше 3000
print('Самый дорогой товар: ', expencive)

low_price = df[df['final_price'] < 3000]  # Фильтрует товары с финальной ценой меньше 3000
print('Самый дешевый товар: ',low_price)

mobile_values = df[df['category'] == 'Mobile']  # Фильтрует товары категории 'Mobile'
print('Фильтр по категории Mobile: ',mobile_values)

sorted_df = df.sort_values(by='final_price', ascending=False)  # Сортирует DataFrame по столбцу 'final_price' в порядке убывания
print('Сортировка по final_price (по убыванию): ', sorted_df)

sum_price = np.sum(df['final_price'])  # Суммирует значения в столбце 'final_price'
print('Сумма всех значений в столбце final_price: ', sum_price)

mean_price = np.mean(df['final_price'])  # Находит среднее значение в столбце 'final_price'
print('Среднее значение в столбце final_price: ', mean_price)   

std_dev_price = np.std(df['final_price'])  # Находит стандартное отклонение в столбце 'final_price'
print('Стандартное отклонение в столбце final_price: ', std_dev_price)