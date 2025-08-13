# DRY — Don’t Repeat Yourself
from kitchen import make_pizza, make_icecreame
from check import check_access_no_params, check_access_with_params, check_access_return, check_access, grow_up

# Пример вызова функций
make_pizza(2, True)
make_icecreame(True, False, 'Anastasiya Yafimuk')

check_access_no_params()
check_access_with_params(20)
result = check_access_return(20)
print(result)   
print(check_access())
age = 16  # Пример глобальной переменной
print(f'До изменения: {age}')
grow_up()
print(f'После изменения: {age}')    

