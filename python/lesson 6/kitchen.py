def make_pizza(cheese_count, has_pepperoni):
    print(f'Добавляем {cheese_count} порции сыра')
    if has_pepperoni:
        print('Добавляем пепперони')
        print('Выпекаем пиццу 25 минут')
    print('Пицца готова!')

def make_icecreame(has_chocolate, has_nuts, name):
    if has_chocolate:
        print('Добавляем шоколад')
    if has_nuts:
        print('Добавляем орехи')
    print(f'Заберите мороеное, {name}') 