import pandas as pd
import pymysql
import os
from keras.models import load_model
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sqlalchemy import create_engine
import sys
import joblib


# Command line arguments for database credentials
DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

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
except Exception as e:
    print(f"Error fetching all data: {e}")

# Preprocessing all data
df_all = df_all.dropna()
X_all = scaler.transform(df_all.drop(['id'], axis=1))

# Generating scores using the model
scores = model.predict(X_all).flatten()

# Invert the scores
inverted_scores = 1 - scores

# Proceed to save the inverted scores to CSV and update the database
score_data = pd.DataFrame({'id': df_all['id'], 'scoreModificationProposed': inverted_scores})
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