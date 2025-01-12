# from datetime import datetime, time, timedelta
import pandas as pd

from userlogic import user_exists, add_user, get_user_id
from metrikalogic import entry_exists, add_entry
from jsonRequest import print_json

all_json_data = []

def send_response(file):
    data = pd.read_excel(file)
    print(data.head(6))
    print(data.tail(5))

    user_in_block = ['Vizor', 'Vizor2', 'Администратор', 'Оператор1', 'super123']



    for i in range(4, len(data)):
        metrica = data.iloc[i, 0:19].tolist()
        
        if metrica[1] not in user_in_block and isinstance(metrica[1], str):
            SheetData = metrica[0]
            this_user = metrica[1]

            if not user_exists(this_user):
                print(this_user)
                add_user(this_user)
                
            operator_id = get_user_id(this_user)  

            if not entry_exists(operator_id, SheetData):
                StatusTimeInPlace = metrica[2]
                StatusTimeBusy = metrica[3]
                StatusTimeBreak = metrica[4]
                StatusTimeGone = metrica[5]
                StatusTimeNotAvailable = metrica[6]
                
                PercentInPlace = metrica[7]
                
                if isinstance(metrica[9], int): 
                    CountIncoming = metrica[9] 
                else: 
                    CountIncoming = 0
                
                if isinstance(metrica[10], str): 
                    LenghtIncoming = metrica[10]
                else: 
                    LenghtIncoming = "00:00:00"
                    
                if isinstance(metrica[11], str): 
                    IncomingAVG = metrica[11]
                else: 
                    IncomingAVG = "00:00:00"
                    
                if isinstance(metrica[12], int): 
                    CountOutgoing = metrica[12]
                else: 
                    CountOutgoing = 0
                    
                if isinstance(metrica[13], str): 
                    LenghtOutgoing = metrica[13]
                else: 
                    LenghtOutgoing = "00:00:00"
                
                if isinstance(metrica[14], str): 
                    OutgoingAVG = metrica[14]
                else: 
                    OutgoingAVG = "00:00:00"
                    
                if isinstance(metrica[15], int): 
                    CountMissed = metrica[15] 
                else: 
                    CountMissed = 0
                
                json_data = {
                    "Date" : str(SheetData),
                    "Operator" : this_user,
                    "operator_id" : operator_id,
                    "Status" : {
                        "StatusTimeInPlace": StatusTimeInPlace,
                        "StatusTimeBusy": StatusTimeBusy,
                        "StatusTimeBreak": StatusTimeBreak,
                        "StatusTimeGone": StatusTimeGone,
                        "StatusTimeNotAvailable": StatusTimeNotAvailable,
                        "PercentInPlace": PercentInPlace
                    },
                    "CallMetrics": {
                        "CountIncoming": CountIncoming,
                        "LenghtIncoming": LenghtIncoming,
                        "IncomingAVG": IncomingAVG,
                        "CountOutgoing": CountOutgoing,
                        "LenghtOutgoing": LenghtOutgoing,
                        "OutgoingAVG": OutgoingAVG,
                        "CountMissed": CountMissed
                    }
                }
                
                all_json_data.append(json_data)
                
                add_entry(operator_id, SheetData)
            else:
                print(f'Экземпляр с operator_id: {operator_id}, data: {SheetData} уже существует')
    
    return all_json_data