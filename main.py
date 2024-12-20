import psycopg2
from psycopg2.extras import RealDictCursor
from config import db_params

def get_user_statistics(date_start_from, date_start_to, users_from_groups, users_from_subordinations):
    # Подзапрос с группировкой по дате и пользователю
    states_query = """
    SELECT date_trunc('day', "TimeOn") as Date, "IDUser", 
           sum("TimeDelta"*(case coalesce("IDUserBaseState", "IDUserState") when 300 then 1 else 0 end)) as UserStateOnlineDuration,
           sum("TimeDelta"*(case coalesce("IDUserBaseState", "IDUserState") when 304 then 1 else 0 end)) as UserStateBusyDuration,
           sum("TimeDelta"*(case coalesce("IDUserBaseState", "IDUserState") when 301 then 1 else 0 end)) as UserStateTimeoutDuration,
           sum("TimeDelta"*(case coalesce("IDUserBaseState", "IDUserState") when 302 then 1 else 0 end)) as UserStateAwayDuration,
           sum("TimeDelta"*(case coalesce("IDUserBaseState", "IDUserState") when 303 then 1 else 0 end)) as UserStateNADuration
    FROM S_UsersStates
    WHERE TimeOn between %s and %s
    %s
    GROUP BY date_trunc('day', TimeOn), IDUser
    ORDER BY IDUser
    """

    # Подзапросы для подсчета входящих соединений
    connections_in_query = """
    SELECT date_trunc('day', TimeStart) as TimeStartDate, IDUser, count(ID) as ConnectionsInCount, sum(Duration) as ConnectionsInDuration, avg(Duration) as ConnectionsInDuration_avg
    FROM S_CMCalls
    WHERE TimeStart between %s and %s AND Direction = 1 AND Duration > 0
    %s
    GROUP BY date_trunc('day', TimeStart), IDUser
    """

    # Подзапросы для подсчета исходящих соединений
    connections_out_query = """
    SELECT date_trunc('day', TimeStart) as TimeStartDate, IDUser, count(ID) as ConnectionsOutCount, sum(Duration) as ConnectionsOutDuration, avg(Duration) as ConnectionsOutDuration_avg
    FROM S_CMCalls
    WHERE TimeStart between %s and %s AND Direction = 2 AND Duration > 0
    %s
    GROUP BY date_trunc('day', TimeStart), IDUser
    """

    # Подзапросы для подсчета потерянных соединений
    connections_lost_query = """
    SELECT date_trunc('day', TimeStart) as TimeStartDate, IDUser, count(ID) as ConnectionsLostCount
    FROM S_CMCalls
    WHERE TimeStart between %s and %s AND Direction = 1 AND Duration = 0
    %s
    GROUP BY date_trunc('day', TimeStart), IDUser
    """

    # Подзапросы для подсчета внутренних сообщений
    inner_chat_messages_query = """
    SELECT date_trunc('day', MessageTime) as Date, IDSender, count(ID) as Count
    FROM S_ChatMessages
    WHERE SessionType <> 4 AND MessageType = 0
    %s
    GROUP BY date_trunc('day', MessageTime), IDSender
    """

    # Подзапросы для подсчета внешних сообщений
    external_chat_messages_query = """
    SELECT date_trunc('day', MessageTime) as Date, IDSender, count(ID) as Count
    FROM S_ChatMessages
    WHERE SessionType = 4 AND MessageType = 0
    %s
    GROUP BY date_trunc('day', MessageTime), IDSender
    """

    # Формирование полного запроса
    main_query = f"""
    SELECT 
        s.Date, s.IDUser, 
        COALESCE(s.UserStateOnlineDuration, 0) + COALESCE(cin.ConnectionsInDuration, 0) + COALESCE(cout.ConnectionsOutDuration, 0) as TotalDuration,
        {states_query},
        {connections_in_query} AS ConnectionsIn,
        {connections_out_query} AS ConnectionsOut,
        {connections_lost_query} AS ConnectionsLost,
        {inner_chat_messages_query} AS InnerChatMessages,
        {external_chat_messages_query} AS ExternalChatMessages
    FROM ({states_query}) s
    LEFT JOIN ({connections_in_query}) cin ON s.Date = cin.TimeStartDate AND s.IDUser = cin.IDUser
    LEFT JOIN ({connections_out_query}) cout ON s.Date = cout.TimeStartDate AND s.IDUser = cout.IDUser
    LEFT JOIN ({connections_lost_query}) cl ON s.Date = cl.TimeStartDate AND s.IDUser = cl.IDUser
    LEFT JOIN ({inner_chat_messages_query}) ichm ON s.Date = ichm.Date AND s.IDUser = ichm.IDSender
    LEFT JOIN ({external_chat_messages_query}) echm ON s.Date = echm.Date AND s.IDUser = echm.IDSender
    WHERE s.Date = %s
    """

    # Подготовка параметров запроса
    params = (
        date_start_from,
        date_start_to,
        f"AND (IDUser > 0) AND (%s like '%|' || IDUser || '|%')",
        date_start_from,
        date_start_to,
        f"AND (IDUser > 0) AND (%s like '%|' || IDUser || '|%')",
        date_start_from,
        date_start_to,
        f"AND (IDUser > 0) AND (%s like '%|' || IDUser || '|%')",
        date_start_from,
        date_start_to,
        f"AND (IDUser > 0) AND (%s like '%|' || IDUser || '|%')",
        date_start_from,
        date_start_to,
        users_from_groups,
        users_from_subordinations,
        users_from_groups,
        users_from_subordinations,
        users_from_groups,
        users_from_subordinations,
        users_from_groups,
        users_from_subordinations
    )

    try:

        # Подключение к базе данных
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Выполнение запроса
        cursor.execute(main_query, params)
        
        # Получение результатов
        results = cursor.fetchall()

        # Закрытие курсора и соединения
        cursor.close()
        conn.close()

        return results

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Ошибка при выполнении запроса: {error}")
        return None

# Пример использования функции
if __name__ == "__main__":

    date_start_from = "2023-01-01"
    date_start_to = "2023-12-31"
    users_from_groups = ""
    users_from_subordinations = ""

    results = get_user_statistics(date_start_from, date_start_to, users_from_groups, users_from_subordinations)

    if results:
        for row in results:
            print(row)
