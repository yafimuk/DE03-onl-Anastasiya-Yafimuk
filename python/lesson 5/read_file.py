# ====== ЧТЕНИЕ ======
# "r" — чтение

file_path = r"python/lesson 5/file.txt"

file = open(file_path, "r")
content = file.read()
print(content)
file.close()

# Чтение одной строки (по очереди)
file =open(file_path, "r")
line = file.readline()
print(line)
file.close()

# Чтение всех строк в список
file =open(file_path, "r")
lines = file.readlines()
print(lines)
file.close()

# ====== ЗАПИСЬ ======
# "w" — запись (перезаписывает или создаёт файл)
file =open(file_path, "w")
file.write("Катя 19")
file.close()

file =open(file_path, "r")
content = file.read()
print(content)
file.close()


file =open(file_path, "w")
file.writelines(['Катя 19\n', 'Юля 26\n'])
file.close()

file =open(file_path, "r")
content = file.read()
print(content)
file.close()

# ====== ЧТЕНИЕ + ЗАПИСЬ ======
# "r+" — файл должен существовать
file =open(file_path, "r+")
file.write("Катя 19")
content = file.read()
print(content)
file.close()

# "a+" — добавление и чтение
file =open(file_path, "a+")
file.writelines(["Катя 19\n"])
file.seek(0) # ставит курсор в начало файла
content = file.readlines()
print(content)




try:
    with open(file_path, "r") as file:
        content = file.read()
        print(content)
except FileNotFoundError:
    print("Файл не найден!")
except PermissionError:
    print("Нет доступа к файлу!")
except IsADirectoryError:
    print("Указан путь к папке, а не к файлу!")
except IOError:
    print("Ошибка ввода-вывода файла!")
except UnicodeDecodeError:
    print("Неправильная кодировка файла!")
finally:
    print("Ошибок нет!") 



with open(file_path, "r") as file:
    persons = file.readlines()
    for person in persons:
        print(person)