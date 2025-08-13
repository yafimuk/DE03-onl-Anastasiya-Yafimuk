def check_access_no_params():
    print('Проверка доступа без параметров')
    age = 16
    if age >= 18:
        print('Доступ разрешен')
    else:
        print('Доступ запрещен')

def check_access_with_params(age):
    print('Проверка доступа c параметрами')
    if age >= 18:
        print('Доступ разрешен')
    else:
        print('Доступ запрещен')

def check_access_return(age):
    print('Проверка возврата значения')
    if age >= 18:
        return 'Доступ разрешен'
    else:
        return 'Доступ запрещен'

age = 16 # Пример глобальной переменной

def check_access():
    print('Проверка доступа с глобальной переменной')
    if age >= 18:
        return 'Доступ разрешен'
    else:
        return 'Доступ запрещен'
    
def grow_up():
    global age  # Используем глобальную переменную
    age += 1
    print(f'Возраст увеличен до {age}')

