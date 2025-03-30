import schedule
from file_script import list_all_files, remove_file_from_query
from DataPreprocessor import send_response
from UserPreprocessor import preprocess_user
from metrikalogic import add_entry
import os
from dotenv import load_dotenv
import json

load_dotenv()

import requests

directory = "/mnt/share/"
all_files = list_all_files(directory)
url_metrics = "http://olegperm.fvds.ru/api/add_metrika"
url_users_acc = "http://olegperm.fvds.ru/api/add_account"
url_to_inf = "http://192.168.1.210:10080/data/getdata/?ProviderName=Security.Users"

def send_data_to_server(url, data):
    headers = {"Authorization": os.environ.get('CROSS_SERVER_INTEGRATION_KEY'), "Content-Type": "application/json"}
    response = requests.post(url, json=data, headers=headers)
    
    if response.status_code == 201:
        print("Данные успешно отправлены!")
        if url == url_metrics:
            data = json.loads(data)
            for part_json in data.get("metriks", []):
                Date = part_json.get("Date")
                Operator = part_json.get("Operator")
                add_entry(Operator, Date)
            
            return(response.status_code)
    else:
        print(f"Ошибка при отправке данных: {response.status_code} {response.json}")
        return(response.status_code)

def fetch_users_data(url_users_acc):
    try:
        response = requests.get(url_users_acc)
        response.raise_for_status()
        data = response.json()

        if "Data" in data["result"]:
            return data["result"]["Data"]
        else:
            print("Ключ 'Data' не найден в ответе.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Произошла ошибка при выполнении запроса: {e}")
        return None

def file_handling():
    response = fetch_users_data(url_to_inf)
    for data in response:
        if preprocess_user(data) != None:
            send_data_to_server(url_users_acc, preprocess_user(data))


    if all_files:
        print("Список всех файлов, которые еще не были обработаны:")
        print(all_files)
        
        for file in all_files:
            if file.endswith('.xls'):
                status = send_data_to_server(url_metrics, send_response(file))
                
                remove_file_from_query(file) if status == 201 else print("Непридвиденное состояние, данные не были отправлены:", status)
    else:
        print("Все файлы уже были обработаны.")
        
def keep_alive():
    print("Alive.")
        
def main():
    schedule.every(1).minutes.do(keep_alive)
    schedule.every().hour.at(":36").do(file_handling)
    
    while True:
        schedule.run_pending()
        
if __name__ == '__main__':
    main()