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
        "password": str(extract_number(data['login'])),
        "prefix": data['Name_F'] + " " + data['Name_I'] + " " + data['Name_O'] if data['Name_F'].upper != 'СВОБОДНО' else 'СВОБОДНО',
        "role": "user",
        "login": data['login'],
        "isActive": True if data['Blocked'] == '0' and data['Name_F'].upper != 'СВОБОДНО' else False,
    }
    
    print(result)
    return result