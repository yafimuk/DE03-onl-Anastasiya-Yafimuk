# по спискам
fruits = ["яблоко", "яблоко", "груша", "вишня"]

for fruit in fruits:
    print(fruit.lower())

# по строкам
word = "Python"

for char in word:
    print(char)

# по диапазону
for i in range(5):
    print(i)

for i in range(2, 10):
    print(i)

for i in range(2, 11, 2):
    print(i)

for i in range(1, 6, 3):
    print(i)

# вложенные циклы
for i in range(0, 1):
    for j in range(0, 1):
        for k in range(0, 1):
            print(i, j, k, sep=":")


# счетчик
count = 0

while count < 5:
    print(count)
    count = count + 1

# выход из цикла и продолжение
while True:
    user_input = input("Введи число либо нажми клавишу q или с:")
    if user_input == "q":
        break
    elif user_input == "c":
        continue
    print(user_input)
