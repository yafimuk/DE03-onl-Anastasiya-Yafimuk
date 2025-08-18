print(10 / 0) # ZeroDivisionError: division by zero

my_list = [1, 2, 3]
print(my_list[5]) # IndexError: list index out of range

with open('non_existent_file.txt', 'r') as file: # FileNotFoundError: [Errno 2] No such file or directory: 'non_existent_file.txt'
    content = file.read()
  
print(int('abc'))  # ValueError: invalid literal for int() with base 10: 'abc'


try:
    print(10 / 0)  # This will raise a ZeroDivisionError
except ZeroDivisionError as e:
    print(f"Error: {e}")

my_list = [1, 2, 3]
try:
    element = my_list[4]  # This will raise an IndexError
except IndexError as e:
    print(f"Error: {e}")   

try:
    with open('non_existent_file.txt', 'r') as file:  # This will raise a FileNotFoundError
        content = file.read()
except FileNotFoundError as e:
    print(f"Error: {e}")     


a = int(input("Введите число: "))  # This will raise a ValueError if input is not a valid integer
b = int(input("Введите число: "))
try:
    result = a/b  # This will raise a ValueError    
except ZeroDivisionError as e:
    print(f"Error: {e}")
else:
    print(f"Result: {result}")
finally:
    print("Завершение обработки.")


file_path = r"lesson 7/файл.txt"
try:
    with open(file_path, "r", encoding='ASCII') as file:
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
else:
    print("Файл успешно прочитан!")    
finally:
    print("Завершение обработки файла.") 
