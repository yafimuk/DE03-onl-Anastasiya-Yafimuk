# ---------- Создание исходного списка ----------
lst = [3, 1, 2, 2, 5, 4]
print("Исходный список:", lst)


# ---------- Добавление элементов ----------
lst.append(15)  # Добавляет элемент в конец списка
print("append(15):", lst)

lst.extend([12, 16, 23, 15])  # Добавляет несколько элементов
print("extend([12, 16, 23, 15]):", lst)

lst.insert(3, 19)  # Вставляет 19 на позицию с индексом 3
print("insert(3, 19):", lst)


# ---------- Удаление элементов ----------
lst.remove(15)  # Удаляет первое вхождение элемента 15
print("remove(15):", lst)

removed_last = lst.pop()  # Удаляет и возвращает последний элемент
removed_index = lst.pop(5)  # Удаляет элемент с индексом 5
lst.pop()  # Удаляет последний элемент
print("pop():", removed_last, removed_index, lst)

# ---------- Очистка ----------
# lst.clear() # Удаляет все элементы из списка
# print("clear():", lst)

# ---------- Подсчёт и копирование ----------
lst_copy = lst.copy()  # Полная копия списка
print("copy()", lst_copy)

lst_count = lst_copy.count(2)  # Количество вхождений элемента 2
print("count(2)", lst_count)

# ---------- Поиск элемента ----------
first2 = lst.index(2)  # Индекс первого вхождения элемента 2
print("index(2)", first2)

# ---------- Сортировка и разворот ----------
lst.reverse()  # Разворачивает список
print("reverse()", lst)

lst.sort()  # Сортирует список по возрастанию
print("sort()", lst)

lst.sort(reverse=True)  # Сортирует список по убыванию
print("sort(reverse=True)", lst)

# ---------- sorted() (создает новый список) ----------
new_sorted = sorted(lst)  # Новый отсортированный список по возрастанию
print("sorted(lst):", new_sorted)

# ---------- Полезные встроенные функции ----------
print("len(lst):", len(lst))
print("sum(lst):", sum(lst))
print("min(lst):", min(lst))
print("max(lst):", max(lst))
