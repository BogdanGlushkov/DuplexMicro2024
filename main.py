from file_script import list_all_files, remove_file_from_query
from DataPreprocessor import send_response
from UserPreprocessor import preprocess_user
from metrikalogic import add_entry
import os
from dotenv import load_dotenv
import json

load_dotenv()

import requests

# Пример использования
directory = "/home/remote/mnt/share/"
all_files = list_all_files(directory)
url_metrics = "http://olegperm.fvds.ru/api/add_metrika" # Путь к endpoint на сервере
url_users_acc = "http://olegperm.fvds.ru/api/add_account"
url_to_inf = "http://192.168.1.210:10080/data/getdata/?ProviderName=Security.Users"

def send_data_to_server(data):
    # Отправляем JSON данные через POST запрос
    headers = {"Authorization": os.environ.get('CROSS_SERVER_INTEGRATION_KEY'), "Content-Type": "application/json"}
    response = requests.post(url_metrics, json=data, headers=headers)
    
    if response.status_code == 201:
        print("Данные успешно отправлены!")
        data = json.loads(data)
        for part_json in data.get("metriks", []):
            Date = part_json.get("Date")
            Operator = part_json.get("Operator")
            add_entry(Operator, Date)
        remove_file_from_query(file)
    else:
        print(f"Ошибка при отправке данных: {response.status_code} {response.json}")

def fetch_users_data(url_users_acc):
    try:
        # Выполняем GET-запрос
        response = requests.get(url_users_acc)
        response.raise_for_status()  # Генерирует исключение, если код состояния не 200
        data = response.json()  # Преобразуем ответ в JSON (словарь или список)
        # Извлекаем данные из ключа "Data"
        if "Data" in data["result"]:
            return data["result"]["Data"]
        else:
            print("Ключ 'Data' не найден в ответе.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")
        return None


response = fetch_users_data(url_to_inf)
for data in response:
    preprocess_user(data)

# Выводим все файлы, которые еще не были обработаны
if all_files:
    print("Список всех файлов, которые еще не были обработаны:")
    print(all_files)
    
    for file in all_files:
        if file.endswith('.xls'):
            send_data_to_server(send_response(file))
else:
    print("Все файлы уже были обработаны.")