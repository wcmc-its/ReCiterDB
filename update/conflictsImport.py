import boto3
import csv
import logging
import pymysql.cursors
import pymysql.err
import sys
import time
import os  # Import the os module

# Delete the existing conflicts.csv file if it exists
if os.path.exists('conflicts.csv'):
    os.remove('conflicts.csv')

# Configure logging to output to stdout
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(message)s')

DB_USERNAME = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')

def connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME):
    """Function to connect to MySQL database"""
    try:
        mysql_db = pymysql.connect(user=DB_USERNAME,
                                   password=DB_PASSWORD,
                                   database=DB_NAME,
                                   host=DB_HOST,
                                   autocommit=True,
                                   local_infile=True,
                                   cursorclass=pymysql.cursors.DictCursor)  # Use DictCursor to fetch results as dictionaries
        logging.info(f"Connected to database server: {DB_HOST}, database: {DB_NAME}, with user: {DB_USERNAME}")
        return mysql_db
    except pymysql.err.MySQLError as err:
        logging.error(f"{time.ctime()} -- Error connecting to the database: {err}")

# Connect to MySQL
mysql_conn = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)

# Query for PMIDs
with mysql_conn.cursor() as mysql_cursor:
    mysql_cursor.execute('''
        SELECT distinct p.pmid AS pmid
        FROM analysis_summary_article p
        LEFT JOIN reporting_conflicts a ON a.pmid = p.pmid
        WHERE a.pmid is null
        and articleYear >= 2017
        LIMIT 90
    ''')
    results = mysql_cursor.fetchall()
    pmids = [result['pmid'] for result in results]  # Use dictionary key to access pmid



# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('PubMedArticle')

# Use client for batch get 
client = dynamodb.meta.client
keys = [{'pmid': pmid} for pmid in pmids]

# Check if keys is empty before making the call
if not keys:
    logging.warning("No PMIDs found to fetch from DynamoDB.")
    exit()
else:
    response = client.batch_get_item(RequestItems={
      'PubMedArticle': {
          'Keys': keys
      }
    })
    items = response['Responses']['PubMedArticle']


response = client.batch_get_item(RequestItems={
  'PubMedArticle': {
      'Keys': keys
  }
})

items = response['Responses']['PubMedArticle']


def get_conflicts(item):
    medline_citation = item.get('pubmedarticle', {}).get('medlinecitation')
    if medline_citation:
        return medline_citation.get('coiStatement', "")
    return ""



# Initial CSV setup
with open('conflicts.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')  # specify tab delimiter here
    writer.writerow(['pmid', 'conflictStatement'])

offset = 0 
limit = 90

while True:
    logging.info(f'Processing offset: {offset}')  # Log the current offset

    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute('''
        SELECT distinct p.pmid AS pmid
        FROM analysis_summary_article p
        LEFT JOIN reporting_conflicts a ON a.pmid = p.pmid
        WHERE a.pmid is null
        and articleYear >= 2017
        LIMIT %s OFFSET %s
    ''', (limit, offset))

    results = mysql_cursor.fetchall()

    if not results:
        logging.info('No more results, breaking out of loop')  # Log the end condition
        break

    pmids = [result['pmid'] for result in results]

    logging.info(f'Fetching data for {len(pmids)} PMIDs from DynamoDB')  # Log the number of PMIDs

    keys = [{'pmid': pmid} for pmid in pmids]
    response = client.batch_get_item(RequestItems={'PubMedArticle': {'Keys': keys}})
    items = response['Responses']['PubMedArticle']

    for item in items:
        conflictStatement = get_conflicts(item)
        with open('conflicts.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter='\t')  # specify tab delimiter here
            writer.writerow([item['pmid'], conflictStatement])

    offset += limit


def load_conflicts(mysql_cursor):
    cwd = os.getcwd()
    load_conflicts_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/conflicts.csv' INTO TABLE reporting_conflicts FIELDS TERMINATED BY '\t' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (pmid,conflictStatement);"
    )
    mysql_cursor.execute(load_conflicts_query)
    print(time.ctime() + "--" + " conflicts.csv file loaded")

    update_query = (
      "UPDATE reporting_conflicts SET conflictsVarchar = CAST(conflictStatement AS CHAR(15000)) where conflictsVarchar is null;"
    )
    
    mysql_cursor.execute(update_query)
    print(time.ctime() + " reporting_conflicts table updated with varchar equivalent")  


if __name__ == '__main__':

    # Create a MySQL connection to the Reciter database
    reciter_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
    with reciter_db.cursor() as reciter_db_cursor:
        load_conflicts(reciter_db_cursor)

    # Close DB connection
    reciter_db.close()

