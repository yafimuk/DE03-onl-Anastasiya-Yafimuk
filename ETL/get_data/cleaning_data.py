import pandas as pd

# Загрузка данных
df = pd.read_csv('ETL/get_data/students.csv')

# 1. Нормализация значений (lowercase, удаление пробелов)
df['Имя'] = df['Имя'].str.strip().str.capitalize()
df['Город'] = df['Город'].str.strip().str.capitalize()

# Приведение возраста к числовому виду
df['Возраст'] = df['Возраст'].replace({'восемнадцать': '18'}).astype(int)

# Приведение пола к единому формату
def normalize_gender(x):
    if pd.isna(x): return None
    x = x.lower()
    if x in ['м', 'муж']: return 'М'
    if x in ['ж', 'жен']: return 'Ж'
    return x.capitalize()
df['Пол'] = df['Пол'].apply(normalize_gender)

# Приведение города к единому формату
df['Город'] = df['Город'].replace({'Minsk': 'Минск', 'мИнск': 'Минск', 'минск': 'Минск', 'Gomel': 'Гомель'})

# 2. Удаление дубликатов (по имени и email)
df = df.drop_duplicates(subset=['Email', 'Баллы'])

# 3. Обработка пропусков
df['Баллы'] = df['Баллы'].fillna(0).astype(int)
df['Email'] = df['Email'].fillna('no_email@unknown.com')
df['Пол'] = df['Пол'].fillna('Не указан')


print(df)
