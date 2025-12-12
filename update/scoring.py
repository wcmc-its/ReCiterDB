import os
import pymysql
import subprocess
from datetime import datetime
import pandas as pd
from keras.models import load_model
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sqlalchemy import create_engine
import sys
import joblib

# Command line arguments for database credentials
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_HOST = os.getenv('DB_HOST')

def run_sql_procedure(cursor, procedure_name):
    start_time = datetime.now()
    try:
        cursor.callproc(procedure_name)
        end_time = datetime.now()
        print(f"Procedure {procedure_name} executed successfully. Time taken: {end_time - start_time}")
    except pymysql.Error as err:
        print(f"Error occurred: {err}")

def main():
    connection = None
    try:
        start_time = datetime.now()
        # Establishing connection to the MySQL database
        connection = pymysql.connect(
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
        print(f"External script executed. Time taken: {datetime.now() - script_start_time}")
        # Executing the final SQL procedure
        run_sql_procedure(cursor, 'scoringOverall')
    except pymysql.Error as err:
        print(f"Database connection failed: {err}")
    finally:
        if connection and connection.open:
            cursor.close()
            connection.close()
            print(f"MySQL connection is closed. Total time taken: {datetime.now() - start_time}")

if __name__ == "__main__":
    main()

# Create a connection using SQLAlchemy
engine = create_engine(f'mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')
# SQL query to fetch all records for scoring
query_all = """
SELECT id,
    scoreCites, scoreCoauthorName, scoreEmail, scoreInstitution,
    scoreJournal, scoreJournalDomain, scoreJournalField, 
    scoreJournalSubfield, scoreKeyword, scoreOrcid, 
    scoreOrcidCoauthor, scoreOrganization, scoreTargetAuthorName, 
    scoreYear
FROM feedback_score_total;
"""

# Load the pre-trained model and the scaler
model = load_model('feedbackScoringModel.keras')
scaler = joblib.load('scaler.save')


# Fetching all data for scoring
try:
    df_all = pd.read_sql(query_all, engine)
    print("All data fetched successfully!")
    print("Number of rows in df_all before dropping 'id':", len(df_all))
except Exception as e:
    print(f"Error fetching all data: {e}")




# Save the 'id' column in a separate variable before dropping it from df_all
if not df_all.empty:
    ids = df_all['id'].copy()  # Save 'id' column
    df_all = df_all.drop(['id'], axis=1)  # Drop 'id' column from df_all
    print("Number of rows in df_all after dropping 'id':", len(df_all))

    if not df_all.empty:
        X_all = scaler.transform(df_all)
        # Generating scores using the model
        scores = model.predict(X_all).flatten()
        # Invert the scores
        inverted_scores = 1 - scores

        # Proceed to save the inverted scores to CSV and update the database
        score_data = pd.DataFrame({'id': ids, 'scoreFeedbackTotal': inverted_scores})
        score_data.to_csv('output.csv', index=False)
        print("Inverted scores saved to 'output.csv'.")
        
        # Database update function
        def load_score_data():
            # Connect to MySQL
            connection = pymysql.connect(
                host=DB_HOST, 
                user=DB_USERNAME, 
                password=DB_PASSWORD, 
                database=DB_NAME, 
                local_infile=True
            )
            cursor = connection.cursor()
            # Truncate the temporary table
            truncate_query = "TRUNCATE TABLE feedback_score_total_temp;"
            cursor.execute(truncate_query)
            print("Temporary table truncated.")
            # Construct the load data query
            cwd = os.getcwd()
            load_score_data_query = (
                "LOAD DATA LOCAL INFILE '" + cwd + "/output.csv' INTO TABLE feedback_score_total_temp FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (id, scoreFeedbackTotal);"
            )
            # Execute the query
            cursor.execute(load_score_data_query)
            connection.commit()
            print("output.csv file loaded")
            # Update the main table from the temporary table
            update_query = """
            UPDATE feedback_score_total ft
            INNER JOIN feedback_score_total_temp ftt ON ft.id = ftt.id
            SET ft.scoreFeedbackTotal = ftt.scoreFeedbackTotal;
            """
            cursor.execute(update_query)
            connection.commit()
            print("Main table updated with new scores.")
            # Close the connection
            cursor.close()
            connection.close()

        # Call the function
        load_score_data()
        # Close the database connection
        engine.dispose()


    else:
        print("DataFrame is empty after dropping 'id'")
else:
    print("DataFrame is empty before dropping 'id'")
