import os
import csv


# Укажите путь к вашему CSV файлу, где хранятся обработанные файлы
csv_file = "csv/processed_files.csv"

# Функция для записи пути к файлу в CSV
def remove_file_from_query(file_path):
    # Проверяем, существует ли CSV файл
    file_exists = os.path.exists(csv_file)
    
    with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        # Если файл только что создан, записываем заголовок
        if not file_exists:
            writer.writerow(["Директория_Файла"])  # Заголовок для CSV
        # Записываем путь к файлу
        writer.writerow([file_path])

# Функция для получения списка обработанных файлов из CSV
def get_processed_files():
    processed_files = set()
    
    # Проверяем, существует ли CSV файл
    if os.path.exists(csv_file):
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader)  # Пропускаем заголовок
            for row in reader:
                if row:  # Проверяем, что строка не пустая
                    processed_files.add(row[0])  # Добавляем путь директории в множество
    return processed_files

# Функция для списка всех файлов, которые еще не были обработаны
def list_all_files(directory):
    all_files = []
    processed_files = get_processed_files()  # Получаем список обработанных файлов

    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            # Если путь файла не содержится в списке обработанных файлов, добавляем его
            if file_path not in processed_files:
                all_files.append(file_path)
    
    return all_files

