import psycopg2
from config import host, user, password, db_name, port


try:
    
    print("Hello 1")
    #connect to db
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name,
        port=port
    )
    
    print("Hello 2")
    
    #cursor
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT version();"
        )
        
        print(f"Output: {cursor.fetchone()}")
        
        print("Hello 3")
    
    
except Exception as _ex:
    print("[INFO] Error while working with PostgreSQL", _ex)
    
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")