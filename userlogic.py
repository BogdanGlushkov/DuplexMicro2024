import csv
import os

# Имя файла для сохранения
csv_file = "csv/users.csv"

# Поля (заголовки таблицы)
fieldnames = ["ID", "Номер_Оператора"]

# Функция для получения следующего id
def get_next_id():
    # Проверяем, существует ли файл
    if os.path.exists(csv_file):
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            max_id = 0
            for row in reader:
                max_id = max(max_id, int(row["ID"]))
            return max_id + 1
    else:
        # Если файла нет, начинаем с 1
        return 0

# Функция для проверки существования номера
def user_exists(operator_number):
    try:
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["Номер_Оператора"] == operator_number:
                    return True
    except FileNotFoundError:
        # Файл ещё не существует
        return False
    return False

def get_user_id(operator_number):
    try:
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["Номер_Оператора"] == operator_number:
                    return row["ID"]
    except FileNotFoundError:
        # Файл ещё не существует
        return "Error"
    return "Error"

# Функция для добавления пользователя
def add_user(operator_number):
    if user_exists(operator_number):
        print(f"Пользователь с номером {operator_number} уже существует.")
        return
    
    new_id = get_next_id()
    # Открываем файл для записи (или создания)
    with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        # Если файл только что создан, записываем заголовок
        if file.tell() == 0:
            writer.writeheader()
        # Добавляем номер оператора
        writer.writerow({"ID": new_id, "Номер_Оператора": operator_number})
        print(f"Пользователь с номером {operator_number} и id {new_id} добавлен в файл {csv_file}.")