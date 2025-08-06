# ---------- Создание словаря ----------
d = {"key1": "value1", "key2": "value2", "key3": "value3"}

# ---------- Копирование и очистка ----------
d_copy = d.copy() # Копирование словаря
print("copy():", d_copy) 

d.clear()   # Очистка словаря
print("clear():", d)

# ---------- Получение значений ----------
print("get('key1'):", d.get("key1"))  

# ---------- Просмотр ключей и значений ----------
print("keys():", list(d.keys()))      
print("values():", list(d.values())) 
print("items():", list(d.items()))

print(type(d.items()))
# ---------- Добавление и обновление ----------
d["key4"] = "value4"
print(d)

d.update({"key2": "hello_value2"})
print(d)

# ---------- Удаление элементов ----------
value_2 = d.pop("key2")
print(value_2)