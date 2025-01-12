import csv

# Имя файла для сохранения
csv_file = "csv/metrics.csv"

# Поля (заголовки таблицы)
fieldnames = ["ID_Оператора", "Дата"]

# Функция для проверки существования записи с данным номером на текущую дату
def entry_exists(operator_number, date):
    try:
        with open(csv_file, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row["ID_Оператора"] == operator_number and row["Дата"] == str(date):
                    return True
    except FileNotFoundError:
        # Файл ещё не существует
        return False
    return False

# Функция для добавления записи в метрику
def add_entry(operator_number, date_today):
    
    if entry_exists(operator_number, date_today):
        print(f"Запись для номера {operator_number} на {date_today} уже существует.")
        return

    # Открываем файл для записи (или создания)
    with open(csv_file, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        # Если файл только что создан, записываем заголовок
        if file.tell() == 0:
            writer.writeheader()
        # Добавляем запись
        writer.writerow({"ID_Оператора": operator_number, "Дата": date_today})
        print(f"Запись для ID {operator_number} на {date_today} добавлена в файл {csv_file}.")
