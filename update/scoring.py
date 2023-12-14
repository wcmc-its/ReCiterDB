import mysql.connector
import subprocess
import os
from datetime import datetime

# Command line arguments for database credentials
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

def run_sql_procedure(cursor, procedure_name):
    start_time = datetime.now()
    try:
        cursor.callproc(procedure_name)
        end_time = datetime.now()
        print(f"Procedure {procedure_name} executed successfully. Time taken: {end_time - start_time}")
    except mysql.connector.Error as err:
        print(f"Error occurred: {err}")

def main():
    try:
        start_time = datetime.now()
        # Establishing connection to the MySQL database
        connection = mysql.connector.connect(
            user=DB_USERNAME,
            password=DB_PASSWORD,
            host=DB_HOST,
            database=DB_NAME
        )
        cursor = connection.cursor()
        print(f"Connected to database. Time taken: {datetime.now() - start_time}")

        # Executing SQL procedures
        run_sql_procedure(cursor, 'scoringIdentity')
        run_sql_procedure(cursor, 'scoringFeedback')

        # Running the external Python script
        script_start_time = datetime.now()
        subprocess.run(["python3", "feedbackScoreArticlesUpdateDatabase.py"])
        print(f"External script executed. Time taken: {datetime.now() - script_start_time}")

        # Executing the final SQL procedure
        run_sql_procedure(cursor, 'scoringOverall')

    except mysql.connector.Error as err:
        print(f"Database connection failed: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print(f"MySQL connection is closed. Total time taken: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()