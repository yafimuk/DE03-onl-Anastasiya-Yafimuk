import requests

# URL public API
url = 'https://jsonplaceholder.typicode.com/users'

# GET-request
response = requests.get(url)

if response.status_code == 200:
    users = response.json()  # data in JSON format
    print(f'Users: {len(users)}')
    for user in users:
        print(f"{user['id']}: {user['name']} ({user['email']})")
else:
    print(f"Error: {response.status_code}")
