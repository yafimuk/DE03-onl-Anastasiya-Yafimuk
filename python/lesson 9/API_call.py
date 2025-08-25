import requests
import pandas as pd

response = requests.get('https://jsonplaceholder.typicode.com/users')

if response.status_code == 200:
    data = response.json()
    df = pd.DataFrame(data)

else:
    print(f"Failed to retrieve data: {response.status_code}")

filtered_df = df[df['name'] == 'Leanne Graham']
print(filtered_df[['name', 'email', 'address']])