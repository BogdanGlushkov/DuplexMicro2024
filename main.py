import psycopg2
from config import db_params
import traceback

from datetime import datetime, timedelta

connection = None
try:
    # Подключение к базе данных
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()
    
    DateStartFrom = datetime.today().strftime('%d.%m.%Y')
    DateStartTo = (datetime.today() + timedelta(days=1)).strftime('%d.%m.%Y')

    # Ваш запрос
    query = """
    select 
        "States".*, 
        round(
            10000.0 *(
            case (
                coalesce(
                "States"."UserStateOnlineDuration", 
                0
                ) + coalesce(
                "States"."UserStateBusyDuration", 
                0
                ) + coalesce(
                "States"."UserStateTimeoutDuration", 
                0
                ) + coalesce(
                "States"."UserStateAwayDuration", 
                0
                ) + coalesce(
                "States"."UserStateNADuration", 
                0
                )
            ) when 0 then null else (
                coalesce(
                "States"."UserStateOnlineDuration", 
                0
                ) + coalesce(
                "States"."UserStateBusyDuration", 
                0
                )
            ) / (
                coalesce(
                "States"."UserStateOnlineDuration", 
                0
                ) + coalesce(
                "States"."UserStateBusyDuration", 
                0
                ) + coalesce(
                "States"."UserStateTimeoutDuration", 
                0
                ) + coalesce(
                "States"."UserStateAwayDuration", 
                0
                ) + coalesce(
                "States"."UserStateNADuration", 
                0
                )
            ) end
            )
        ):: double precision / 100.0 as "UserStateOnlinePercent", 
        round(
            10000.0 *(
            case (
                coalesce(
                "States"."UserStateOnlineDuration", 
                0
                ) + coalesce(
                "States"."UserStateBusyDuration", 
                0
                ) + coalesce(
                "States"."UserStateTimeoutDuration", 
                0
                ) + coalesce(
                "States"."UserStateAwayDuration", 
                0
                ) + coalesce(
                "States"."UserStateNADuration", 
                0
                )
            ) when 0 then null else (
                coalesce(
                "States"."UserStateTimeoutDuration", 
                0
                ) + coalesce(
                "States"."UserStateAwayDuration", 
                0
                ) + coalesce(
                "States"."UserStateNADuration", 
                0
                )
            ) / (
                coalesce(
                "States"."UserStateOnlineDuration", 
                0
                ) + coalesce(
                "States"."UserStateBusyDuration", 
                0
                ) + coalesce(
                "States"."UserStateTimeoutDuration", 
                0
                ) + coalesce(
                "States"."UserStateAwayDuration", 
                0
                ) + coalesce(
                "States"."UserStateNADuration", 
                0
                )
            ) end
            )
        ):: double precision / 100.0 as "UserStateNotOnlinePercent", 
        "ConnectionsIn"."ConnectionsInCount", 
        "ConnectionsIn"."ConnectionsInDuration", 
        case "ConnectionsIn"."ConnectionsInCount" > 0 when true then "ConnectionsIn"."ConnectionsInDuration_avg" else null end as "InAvg", 
        "ConnectionsOut"."ConnectionsOutCount", 
        "ConnectionsOut"."ConnectionsOutDuration", 
        case "ConnectionsOut"."ConnectionsOutCount" > 0 when true then "ConnectionsOut"."ConnectionsOutDuration_avg" else null end as "OutAvg", 
        "ConnectionsLost"."ConnectionsLostCount", 
        coalesce(
            "States"."UserStateOnlineDuration", 
            0
        ) - coalesce(
            "ConnectionsIn"."ConnectionsInDuration", 
            0
        ) - coalesce(
            "ConnectionsOut"."ConnectionsOutDuration", 
            0
        ) as "DownTimeDuration", 
        trunc(
            10000.0 *(
            case "States"."UserStateOnlineDuration" when 0 then null when null then null else (
                coalesce(
                "States"."UserStateOnlineDuration", 
                0
                ) - coalesce(
                "ConnectionsIn"."ConnectionsInDuration", 
                0
                ) - coalesce(
                "ConnectionsOut"."ConnectionsOutDuration", 
                0
                )
            )/ "States"."UserStateOnlineDuration" end
            )
        ):: double precision / 100.0 as "DownTimePercent", 
        case coalesce(
            "ConnectionsIn"."ConnectionsInCount", 
            0
        )+ coalesce(
            "ConnectionsOut"."ConnectionsOutCount", 
            0
        ) when 0 then null else (
            coalesce(
            "States"."UserStateOnlineDuration", 
            0
            ) - coalesce(
            "ConnectionsIn"."ConnectionsInDuration", 
            0
            ) - coalesce(
            "ConnectionsOut"."ConnectionsOutDuration", 
            0
            )
        )/(
            coalesce(
            "ConnectionsIn"."ConnectionsInCount", 
            0
            )+ coalesce(
            "ConnectionsOut"."ConnectionsOutCount", 
            0
            )
        ) end as "AvgDownTimeDuration", 
        "InnerChatMessages"."Count" as "InnerChatMessagesCount", 
        "ExternalChatMessages"."Count" as "ExternalChatMessagesCount" 
        from 
        (
            select 
            date_trunc('day', "TimeOn") as "Date", 
            "IDUser", 
            sum(
                "TimeDelta" *(
                case coalesce(
                    "IDUserBaseState", "IDUserState"
                ) when 300 then 1 else 0 end
                )
            ) as "UserStateOnlineDuration", 
            sum(
                "TimeDelta" *(
                case coalesce(
                    "IDUserBaseState", "IDUserState"
                ) when 304 then 1 else 0 end
                )
            ) as "UserStateBusyDuration", 
            sum(
                "TimeDelta" *(
                case coalesce(
                    "IDUserBaseState", "IDUserState"
                ) when 301 then 1 else 0 end
                )
            ) as "UserStateTimeoutDuration", 
            sum(
                "TimeDelta" *(
                case coalesce(
                    "IDUserBaseState", "IDUserState"
                ) when 302 then 1 else 0 end
                )
            ) as "UserStateAwayDuration", 
            sum(
                "TimeDelta" *(
                case coalesce(
                    "IDUserBaseState", "IDUserState"
                ) when 303 then 1 else 0 end
                )
            ) as "UserStateNADuration" 
            from 
            "S_UsersStates" 
            where 
            (
                "TimeOn" between '2024-12-05'
                and '2024-12-06'
            ) $$$iif == p UsersFromGroups v vtNull $$$true $$$false 
            and (
                ("IDUser" > 0) 
                and (
                : UsersFromGroups like '%|' || "IDUser" || '|%'
                )
            ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false 
            and (
                ("IDUser" > 0) 
                and (
                : UsersFromSubordinations like '%|' || "IDUser" || '|%'
                )
            ) $$$end 
            group by 
            date_trunc('day', "TimeOn"), 
            "IDUser" 
            order by 
            "IDUser"
        ) as "States" 
        left join (
            select 
            date_trunc('day', "TimeStart") as "TimeStartDate", 
            "IDUser", 
            count("ID") as "ConnectionsInCount", 
            sum("Duration") as "ConnectionsInDuration", 
            avg("Duration") as "ConnectionsInDuration_avg" 
            from 
            "S_CMCalls" 
            where 
            (
                "TimeStart" between '2024-12-05'
                and '2024-12-06'
            ) 
            and ("Direction" = 1) 
            and ("Duration" > 0) $$$iif == p UsersFromGroups v vtNull $$$true $$$false 
            and (
                ("IDUser" > 0) 
                and (
                : UsersFromGroups like '%|' || "IDUser" || '|%'
                )
            ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false 
            and (
                ("IDUser" > 0) 
                and (
                : UsersFromSubordinations like '%|' || "IDUser" || '|%'
                )
            ) $$$end 
            group by 
            date_trunc('day', "TimeStart"), 
            "IDUser"
        ) as "ConnectionsIn" on "States"."IDUser" = "ConnectionsIn"."IDUser" 
        and "States"."Date" = "ConnectionsIn"."TimeStartDate" 
        left join (
            select 
            date_trunc('day', "TimeStart") as "TimeStartDate", 
            "IDUser", 
            count("ID") as "ConnectionsLostCount" 
            from 
            "S_CMCalls" 
            where 
            (
                "TimeStart" between '2024-12-05'
                and '2024-12-06'
            ) 
            and ("Direction" = 1) 
            and ("Duration" = 0) $$$iif == p UsersFromGroups v vtNull $$$true $$$false 
            and (
                ("IDUser" > 0) 
                and (
                : UsersFromGroups like '%|' || "IDUser" || '|%'
                )
            ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false 
            and (
                ("IDUser" > 0) 
                and (
                : UsersFromSubordinations like '%|' || "IDUser" || '|%'
                )
            ) $$$end 
            group by 
            date_trunc('day', "TimeStart"), 
            "IDUser"
        ) as "ConnectionsLost" on "States"."IDUser" = "ConnectionsLost"."IDUser" 
        and "States"."Date" = "ConnectionsLost"."TimeStartDate" 
        left join (
            select 
            date_trunc('day', "TimeStart") as "TimeStartDate", 
            "IDUser", 
            count("ID") as "ConnectionsOutCount", 
            sum("Duration") as "ConnectionsOutDuration", 
            avg("Duration") as "ConnectionsOutDuration_avg" 
            from 
            "S_CMCalls" 
            where 
            (
                "TimeStart" between '2024-12-05'
                and '2024-12-06'
            ) 
            and ("Direction" = 2) $$$iif == p UsersFromGroups v vtNull $$$true $$$false 
            and (
                ("IDUser" > 0) 
                and (
                : UsersFromGroups like '%|' || "IDUser" || '|%'
                )
            ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false 
            and (
                ("IDUser" > 0) 
                and (
                : UsersFromSubordinations like '%|' || "IDUser" || '|%'
                )
            ) $$$end 
            group by 
            date_trunc('day', "TimeStart"), 
            "IDUser"
        ) as "ConnectionsOut" on "States"."IDUser" = "ConnectionsOut"."IDUser" 
        and "States"."Date" = "ConnectionsOut"."TimeStartDate" 
        left join (
            select 
            date_trunc('day', "MessageTime") as "Date", 
            "IDSender", 
            count("ID") as "Count" 
            from 
            "S_ChatMessages" 
            where 
            "SessionType" <> 4 
            and "MessageType" = 0 $$$iif == p UsersFromGroups v vtNull $$$true $$$false 
            and (
                ("IDSender" > 0) 
                and (
                : UsersFromGroups like '%|' || "IDSender" || '|%'
                )
            ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false 
            and (
                ("IDSender" > 0) 
                and (
                : UsersFromSubordinations like '%|' || "IDSender" || '|%'
                )
            ) $$$end 
            group by 
            date_trunc('day', "MessageTime"), 
            "IDSender"
        ) as "InnerChatMessages" on "States"."IDUser" = "InnerChatMessages"."IDSender" 
        and "States"."Date" = "InnerChatMessages"."Date" 
        left join (
            select 
            date_trunc('day', "MessageTime") as "Date", 
            "IDSender", 
            count("ID") as "Count" 
            from 
            "S_ChatMessages" 
            where 
            "SessionType" = 4 
            and "MessageType" = 0 $$$iif == p UsersFromGroups v vtNull $$$true $$$false 
            and (
                ("IDSender" > 0) 
                and (
                : UsersFromGroups like '%|' || "IDSender" || '|%'
                )
            ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false 
            and (
                ("IDSender" > 0) 
                and (
                : UsersFromSubordinations like '%|' || "IDSender" || '|%'
                )
            ) $$$end 
            group by 
            date_trunc('day', "MessageTime"), 
            "IDSender"
        ) as "ExternalChatMessages" on "States"."IDUser" = "ExternalChatMessages"."IDSender" 
        and "States"."Date" = "ExternalChatMessages"."Date"
    """

    # Выполнение запроса
    cursor.execute(query)

    # Получение данных
    rows = cursor.fetchall()

    # Вывод данных
    for row in rows:
        print(row)

except psycopg2.Error as e:
    print(f"Ошибка при подключении к базе данных: {e}")
    traceback.print_exc()

except Exception as error:
    print(f"Ошибка: {error}")
    traceback.print_exc()

finally:
    # Закрытие соединения
    if connection:
        cursor.close()
        connection.close()
        print("Соединение закрыто")