import json
from datetime import datetime

def print_json(data):
    print(json.dumps(data, ensure_ascii=False, indent=4))