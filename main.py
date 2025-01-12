from file_script import list_all_files, remove_file_from_query
from DataPreprocessor import send_response

import requests

# Пример использования
directory = "/home/remote/mnt/share/"  # Укажите вашу директорию
all_files = list_all_files(directory)
url = "http://example.com/your-endpoint" # Путь к endpoint на сервере

def send_data_to_server(data):
    # Отправляем JSON данные через POST запрос
    response = requests.post(url, json=data)
    
    if response.status_code == 200:
        print("Данные успешно отправлены!")
        remove_file_from_query(file)
    else:
        print(f"Ошибка при отправке данных: {response.status_code}")

# Выводим все файлы, которые еще не были обработаны
if all_files:
    print("Список всех файлов, которые еще не были обработаны:")
    for file in all_files:
        send_data_to_server(send_response(file))
else:
    print("Все файлы уже были обработаны.")