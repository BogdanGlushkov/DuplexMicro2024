import re

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
        "prefix": data['Name_F'] + " " + data['Name_I'] + " " + data['Name_O'] if 'СВОБОДНО' not in data['Name_F'].upper + " " + data['Name_I'].upper + " " + data['Name_O'].upper else 'СВОБОДНО',
        "role": "user",
        "login": data['Login'],
        "isActive": True if data['Blocked'] == '0' and data['Name_F'].upper != 'СВОБОДНО' else False,
    }
    
    print(result)
    return result