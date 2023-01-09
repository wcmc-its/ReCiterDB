import time
import os
import pymysql.cursors
import pymysql.err
 
import datetime, time 
 
def run_sql_file(filename, connection): 
    ''' 
    The function takes a filename and a connection as input 
    and will run the SQL query on the given connection   
    ''' 
    MYSQL_OPTION_MULTI_STATEMENTS_ON = 0
    start = time.time() 
    print("Start executing: " + filename + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M"))) 
    cursor = connection.cursor() 
    with open(filename, encoding="utf-8") as f:
        commands = f.read().rstrip(';').split(';')
        print(commands)
    for command in commands:
        #print(command)
        #print("------------")
        cursor.execute(command)
        connection.commit() 
     
    end = time.time() 
    print("Time elapsed to run the query:") 
    print(str((end - start)*1000) + ' ms')
     
def connect_mysql_server(username, db_password, db_hostname, database_name):
    """Establish a connection to MySQL or MariaDB server. This function is
    depandend on the PyMySQL library.
    See: https://github.com/PyMySQL/PyMySQL
    Args:
        username (string): username of the database user.
        password (string): password of the database user.
        db_hostname (string): hostname or IP address of the database server.
        database_name (string): the name of the database we are connecting to.
    Returns:
        MySQLConnection object.
    """
    return pymysql.connect(user=username,
                           password=db_password,
                           host=db_hostname,
                           database=database_name)
     
 
def main():   
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')
    
    print(DB_NAME)
    print(DB_HOST)

    databaseConnection = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)  
    run_sql_file("createDatabaseTableReciterDb.sql", databaseConnection)     
    run_sql_file("insertBaselineDataReciterDb.sql", databaseConnection)     
    run_sql_file("createEventsProceduresReciterDb.sql", databaseConnection) 
    databaseConnection.close() 
     
if __name__ == "__main__": 
    main() 
