import json
from mysql.connector import connect, Error

def insert_data(cursor, table_name, data):
    try:
        json_data = json.dumps(data['data'])
        query = f"INSERT INTO {table_name} (timestamp, data) VALUES (%s, %s)"
        cursor.execute(query, (data['timestamp'], json_data))
    except Error as e:
        print(f"Error inserting data into {table_name}: {e}")

def sink_to_mysql(data, mysql_config, table_name):
    connection = None
    try:
        connection = connect(
            host=mysql_config['host'],
            user=mysql_config['user'],
            password=mysql_config['password'],
            database=mysql_config['database']
        )
        if connection.is_connected():
            cursor = connection.cursor()
            insert_data(cursor, table_name, data)
            connection.commit()  # Commit the transaction using the connection object
            cursor.close()
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    finally:
        if connection and connection.is_connected():
            connection.close()
