import requests

recommendations_url = 'http://127.0.0.1:8000'

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

# Проверка для пользователя, у которого был взаимодействие
params = {"user_id": 0}

resp = requests.post(recommendations_url+'/recommendations', headers=headers, params=params)

if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    print(f"status code: {resp.status_code}")
    
print(recs)

# Проверка для пользователя с холодным стартом. Должны быть рекомендованы треки из топ-100
params = {"user_id": 1179649}

resp = requests.post(recommendations_url+'/recommendations', headers=headers, params=params)

if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    print(f"status code: {resp.status_code}")
    
print(recs)