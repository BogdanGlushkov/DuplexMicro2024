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
    query = f"""
    SELECT "States".*,
		 round( 10000.0 *(
	CASE ( coalesce( "States"."UserStateOnlineDuration", 0 ) + coalesce( "States"."UserStateBusyDuration", 0 ) + coalesce( "States"."UserStateTimeoutDuration", 0 ) + coalesce( "States"."UserStateAwayDuration", 0 ) + coalesce( "States"."UserStateNADuration", 0 ) )
	WHEN 0 THEN
	null
	ELSE ( coalesce( "States"."UserStateOnlineDuration", 0 ) + coalesce( "States"."UserStateBusyDuration", 0 ) ) / ( coalesce( "States"."UserStateOnlineDuration", 0 ) + coalesce( "States"."UserStateBusyDuration", 0 ) + coalesce( "States"."UserStateTimeoutDuration", 0 ) + coalesce( "States"."UserStateAwayDuration", 0 ) + coalesce( "States"."UserStateNADuration", 0 ) )
	END ) ):: double precision / 100.0 AS "UserStateOnlinePercent", round( 10000.0 *(
	CASE ( coalesce( "States"."UserStateOnlineDuration", 0 ) + coalesce( "States"."UserStateBusyDuration", 0 ) + coalesce( "States"."UserStateTimeoutDuration", 0 ) + coalesce( "States"."UserStateAwayDuration", 0 ) + coalesce( "States"."UserStateNADuration", 0 ) )
	WHEN 0 THEN
	null
	ELSE ( coalesce( "States"."UserStateTimeoutDuration", 0 ) + coalesce( "States"."UserStateAwayDuration", 0 ) + coalesce( "States"."UserStateNADuration", 0 ) ) / ( coalesce( "States"."UserStateOnlineDuration", 0 ) + coalesce( "States"."UserStateBusyDuration", 0 ) + coalesce( "States"."UserStateTimeoutDuration", 0 ) + coalesce( "States"."UserStateAwayDuration", 0 ) + coalesce( "States"."UserStateNADuration", 0 ) )
	END ) ):: double precision / 100.0 AS "UserStateNotOnlinePercent", "ConnectionsIn"."ConnectionsInCount", "ConnectionsIn"."ConnectionsInDuration",
	CASE "ConnectionsIn"."ConnectionsInCount" > 0
	WHEN true THEN
	"ConnectionsIn"."ConnectionsInDuration_avg"
	ELSE null
	END AS "InAvg", "ConnectionsOut"."ConnectionsOutCount", "ConnectionsOut"."ConnectionsOutDuration",
	CASE "ConnectionsOut"."ConnectionsOutCount" > 0
	WHEN true THEN
	"ConnectionsOut"."ConnectionsOutDuration_avg"
	ELSE null
	END AS "OutAvg", "ConnectionsLost"."ConnectionsLostCount", coalesce( "States"."UserStateOnlineDuration", 0 ) - coalesce( "ConnectionsIn"."ConnectionsInDuration", 0 ) - coalesce( "ConnectionsOut"."ConnectionsOutDuration", 0 ) AS "DownTimeDuration", trunc( 10000.0 *(
	CASE "States"."UserStateOnlineDuration"
	WHEN 0 THEN
	null
	WHEN NULL THEN
	null
	ELSE ( coalesce( "States"."UserStateOnlineDuration", 0 ) - coalesce( "ConnectionsIn"."ConnectionsInDuration", 0 ) - coalesce( "ConnectionsOut"."ConnectionsOutDuration", 0 ) )/ "States"."UserStateOnlineDuration"
	END ) ):: double precision / 100.0 AS "DownTimePercent",
	CASE coalesce( "ConnectionsIn"."ConnectionsInCount", 0 )+ coalesce( "ConnectionsOut"."ConnectionsOutCount", 0 )
	WHEN 0 THEN
	null
	ELSE ( coalesce( "States"."UserStateOnlineDuration", 0 ) - coalesce( "ConnectionsIn"."ConnectionsInDuration", 0 ) - coalesce( "ConnectionsOut"."ConnectionsOutDuration", 0 ) )/( coalesce( "ConnectionsIn"."ConnectionsInCount", 0 )+ coalesce( "ConnectionsOut"."ConnectionsOutCount", 0 ) )
	END AS "AvgDownTimeDuration", "InnerChatMessages"."Count" AS "InnerChatMessagesCount", "ExternalChatMessages"."Count" AS "ExternalChatMessagesCount"
FROM 
	(SELECT date_trunc('day', "TimeOn") AS "Date", "IDUser", sum( "TimeDelta" *(
		CASE coalesce( "IDUserBaseState", "IDUserState" )
		WHEN 300 THEN
		1
		ELSE 0
		END ) ) AS "UserStateOnlineDuration", sum( "TimeDelta" *(
		CASE coalesce( "IDUserBaseState", "IDUserState" )
		WHEN 304 THEN
		1
		ELSE 0
		END ) ) AS "UserStateBusyDuration", sum( "TimeDelta" *(
		CASE coalesce( "IDUserBaseState", "IDUserState" )
		WHEN 301 THEN
		1
		ELSE 0
		END ) ) AS "UserStateTimeoutDuration", sum( "TimeDelta" *(
		CASE coalesce( "IDUserBaseState", "IDUserState" )
		WHEN 302 THEN
		1
		ELSE 0
		END ) ) AS "UserStateAwayDuration", sum( "TimeDelta" *(
		CASE coalesce( "IDUserBaseState", "IDUserState" )
		WHEN 303 THEN
		1
		ELSE 0
		END ) ) AS "UserStateNADuration"
	FROM "S_UsersStates"
	WHERE ( "TimeOn"
		BETWEEN {DateStartFrom}
			AND {DateStartTo} ) $$$iif == p UsersFromGroups v vtNull $$$true $$$false
			AND ( ("IDUser" > 0)
			AND ( : UsersFromGroups LIKE '%|' || "IDUser" || '|%' ) ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false
			AND ( ("IDUser" > 0)
			AND ( : UsersFromSubordinations LIKE '%|' || "IDUser" || '|%' ) ) $$$end
	GROUP BY  date_trunc('day', "TimeOn"), "IDUser"
	ORDER BY  "IDUser" ) AS "States"
LEFT JOIN 
	(SELECT date_trunc('day', "TimeStart") AS "TimeStartDate", "IDUser", count("ID") AS "ConnectionsInCount", sum("Duration") AS "ConnectionsInDuration", avg("Duration") AS "ConnectionsInDuration_avg"
	FROM "S_CMCalls"
	WHERE ( "TimeStart"
		BETWEEN {DateStartFrom}
			AND {DateStartTo} )
			AND ("Direction" = 1)
			AND ("Duration" > 0) $$$iif == p UsersFromGroups v vtNull $$$true $$$false
			AND ( ("IDUser" > 0)
			AND ( : UsersFromGroups LIKE '%|' || "IDUser" || '|%' ) ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false
			AND ( ("IDUser" > 0)
			AND ( : UsersFromSubordinations LIKE '%|' || "IDUser" || '|%' ) ) $$$end
	GROUP BY  date_trunc('day', "TimeStart"), "IDUser" ) AS "ConnectionsIn"
	ON "States"."IDUser" = "ConnectionsIn"."IDUser"
		AND "States"."Date" = "ConnectionsIn"."TimeStartDate"
LEFT JOIN 
	(SELECT date_trunc('day', "TimeStart") AS "TimeStartDate", "IDUser", count("ID") AS "ConnectionsLostCount"
	FROM "S_CMCalls"
	WHERE ( "TimeStart"
		BETWEEN {DateStartFrom}
			AND {DateStartTo} )
			AND ("Direction" = 1)
			AND ("Duration" = 0) $$$iif == p UsersFromGroups v vtNull $$$true $$$false
			AND ( ("IDUser" > 0)
			AND ( : UsersFromGroups LIKE '%|' || "IDUser" || '|%' ) ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false
			AND ( ("IDUser" > 0)
			AND ( : UsersFromSubordinations LIKE '%|' || "IDUser" || '|%' ) ) $$$end
	GROUP BY  date_trunc('day', "TimeStart"), "IDUser" ) AS "ConnectionsLost"
	ON "States"."IDUser" = "ConnectionsLost"."IDUser"
		AND "States"."Date" = "ConnectionsLost"."TimeStartDate"
LEFT JOIN 
	(SELECT date_trunc('day', "TimeStart") AS "TimeStartDate", "IDUser", count("ID") AS "ConnectionsOutCount", sum("Duration") AS "ConnectionsOutDuration", avg("Duration") AS "ConnectionsOutDuration_avg"
	FROM "S_CMCalls"
	WHERE ( "TimeStart"
		BETWEEN {DateStartFrom}
			AND {DateStartTo} )
			AND ("Direction" = 2) $$$iif == p UsersFromGroups v vtNull $$$true $$$false
			AND ( ("IDUser" > 0)
			AND ( : UsersFromGroups LIKE '%|' || "IDUser" || '|%' ) ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false
			AND ( ("IDUser" > 0)
			AND ( : UsersFromSubordinations LIKE '%|' || "IDUser" || '|%' ) ) $$$end
	GROUP BY  date_trunc('day', "TimeStart"), "IDUser" ) AS "ConnectionsOut"
	ON "States"."IDUser" = "ConnectionsOut"."IDUser"
		AND "States"."Date" = "ConnectionsOut"."TimeStartDate"
LEFT JOIN 
	(SELECT date_trunc('day', "MessageTime") AS "Date", "IDSender", count("ID") AS "Count"
	FROM "S_ChatMessages"
	WHERE "SessionType" <> 4
			AND "MessageType" = 0 $$$iif == p UsersFromGroups v vtNull $$$true $$$false
			AND ( ("IDSender" > 0)
			AND ( : UsersFromGroups LIKE '%|' || "IDSender" || '|%' ) ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false
			AND ( ("IDSender" > 0)
			AND ( : UsersFromSubordinations LIKE '%|' || "IDSender" || '|%' ) ) $$$end
	GROUP BY  date_trunc('day', "MessageTime"), "IDSender" ) AS "InnerChatMessages"
	ON "States"."IDUser" = "InnerChatMessages"."IDSender"
		AND "States"."Date" = "InnerChatMessages"."Date"
LEFT JOIN 
	(SELECT date_trunc('day', "MessageTime") AS "Date", "IDSender", count("ID") AS "Count"
	FROM "S_ChatMessages"
	WHERE "SessionType" = 4
			AND "MessageType" = 0 $$$iif == p UsersFromGroups v vtNull $$$true $$$false
			AND ( ("IDSender" > 0)
			AND ( : UsersFromGroups LIKE '%|' || "IDSender" || '|%' ) ) $$$end $$$iif == p UsersFromSubordinations v vtString |*| $$$true $$$false
			AND ( ("IDSender" > 0)
			AND ( : UsersFromSubordinations LIKE '%|' || "IDSender" || '|%' ) ) $$$end
	GROUP BY  date_trunc('day', "MessageTime"), "IDSender" ) AS "ExternalChatMessages"
	ON "States"."IDUser" = "ExternalChatMessages"."IDSender"
		AND "States"."Date" = "ExternalChatMessages"."Date"
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