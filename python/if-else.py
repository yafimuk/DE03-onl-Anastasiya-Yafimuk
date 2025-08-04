# условия
age = int(input("Введите возраст:"))

expected_age = 0

if 0 < age <= 6:
    expected_age = 18 - age
    if expected_age == 13:
        print(f"Через {expected_age} лет")
        print("Через лет", expected_age)
elif age > 6 and age < 18:
    print("подросток")
elif age < 0 or age > 120:
    print("введите корректный возраст")
else:
    print("взрослый")
