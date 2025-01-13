from file_script import list_all_files, remove_file_from_query
from DataPreprocessor import send_response
from metrikalogic import add_entry
import os
from dotenv import load_dotenv
import json

load_dotenv()

import requests

# Пример использования
directory = "/home/remote/mnt/share/"
all_files = list_all_files(directory)
url = "http://olegperm.fvds.ru/api/add_metrika" # Путь к endpoint на сервере

def send_data_to_server(data):
    # Отправляем JSON данные через POST запрос
    headers = {"Authorization": os.environ.get('CROSS_SERVER_INTEGRATION_KEY'), "Content-Type": "application/json"}
    response = requests.post(url, json=data, headers=headers)
    
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

# Выводим все файлы, которые еще не были обработаны
if all_files:
    print("Список всех файлов, которые еще не были обработаны:")
    for file in all_files:
        if file.endswith('.xls'):
            send_data_to_server(send_response(file))
else:
    print("Все файлы уже были обработаны.")