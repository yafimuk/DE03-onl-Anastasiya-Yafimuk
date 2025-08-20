import numpy as np

array = np.array([1, 2, 3, 4, 5])

print(array)

data = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
print(data)

print(array[0])  # Выводит первый элемент массива
print(array[1:4])  # Выводит элементы с 1 по 3

array_squared = array ** 2
print(array_squared)  # Выводит массив, где каждый элемент возведен в квадрат


print(np.sum(array))  # Выводит сумму всех элементов массива
print(np.mean(array))  # Выводит среднее значение элементов массива
print(np.max(array))  # Выводит максимальное значение в массиве
print(np.min(array))  # Выводит минимальное значение в массиве
print(np.std(array))  # Выводит стандартное отклонение элементов массива
print(np.var(array))  # Выводит дисперсию элементов массива