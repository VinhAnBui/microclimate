import mysql.connector
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(host_name,user_name,user_password,db_name)
        if connection.is_connected():
            print("Connection successful!")
    except Error as e:
        print(f"The error '{e}' occurred")
    
    return connection

# Function to execute a read query
def execute_read_query(connection, query, data=None):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query, data)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
def main():
    hostName = input("hostname: ")  # Localhost
    userName = input( "user_name: ")
    userPassword = getpass.getpass("password: ")
    dbName = getpass.getpass("db_name: ")
    connection = create_connection(host_name, user_name, user_password, db_name)
    
    query = input ("query: ")
    execute_read_query(connection, query, )
    
    connection.close()
main()