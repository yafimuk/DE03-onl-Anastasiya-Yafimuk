numbers_str = input("Введите числа через пробел: ")

numbers_list = list(map(int, numbers_str.split())) # Преобразуем строку в список чисел

unique_sorted_list = sorted(set(numbers_list)) # Удаляем дубликаты и сортируем

print("Уникальные числа в порядке возрастания:", unique_sorted_list)