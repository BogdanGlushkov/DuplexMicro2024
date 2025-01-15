import re

user_in_block = ['Vizor', 'Vizor2', 'Администратор', 'Оператор1', 'super123', 'test', 'test1', 'TestSIP', 'test2']

def extract_number(text):
    match = re.search(r'\d+', text)  # \d+ находит одно или более чисел
    if match:
        return match.group(0)  # Возвращаем найденное число
    else:
        return 123321  # Если число не найдено

def preprocess_user(data):
    result = {
        "user_id_inf": data['ID'],
        "password": str(extract_number(data['Login'])),
        "prefix": data['Name_F'] + " " + data['Name_I'] + " " + data['Name_O'] if 'СВОБОДНО' not in (data['Name_F'] + " " + data['Name_I'] + " " + data['Name_O']) else 'СВОБОДНО',
        "role": "user",
        "login": data['Login'],
        "isActive": True if data['Blocked'] == '0' and 'СВОБОДНО' not in (data['Name_F'] + " " + data['Name_I'] + " " + data['Name_O']) else False,
    }
    
    if data['Login'] not in user_in_block and 'test' not in data['Login']:
        return result
    else:
        return None
    