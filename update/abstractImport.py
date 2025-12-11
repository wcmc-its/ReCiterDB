import boto3
import csv
import logging
import pymysql.cursors
import pymysql.err
import sys
import time
import os  # Import the os module

# Delete the existing abstract.csv file if it exists
if os.path.exists('abstract.csv'):
    os.remove('abstract.csv')

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
        LEFT JOIN reporting_abstracts a ON a.pmid = p.pmid
        WHERE a.pmid is null 
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

def get_abstract(item):
    medline_citation = item.get('pubmedarticle', {}).get('medlinecitation')
    if medline_citation:
        article = medline_citation.get('article')
        if article:
            publication_abstract = article.get('publicationAbstract')
            if publication_abstract:
                abstract_texts = []
                for abstract in publication_abstract['abstractTexts']:
                    label = abstract.get('abstractTextLabel')
                    label_text = label + ": " if label else ""
                    text = abstract.get('abstractText')
                    if text:
                        abstract_texts.append(label_text + text)
                return " ".join(abstract_texts) if abstract_texts else ""
            else:
                return ""
        else:
            return ""
    else:
        return ""

# Initial CSV setup
with open('abstract.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f, delimiter='\t')  # specify tab delimiter here
    writer.writerow(['pmid', 'abstract'])

offset = 0 
limit = 90

while True:
    logging.info(f'Processing offset: {offset}')  # Log the current offset
    with mysql_conn.cursor() as mysql_cursor:
        mysql_cursor.execute('''
            SELECT distinct p.pmid AS pmid
            FROM analysis_summary_article p
            LEFT JOIN reporting_abstracts a ON a.pmid = p.pmid
            WHERE a.pmid is null 
            LIMIT %s OFFSET %s
        ''', (limit, offset))
        results = mysql_cursor.fetchall()
        pmids = [result['pmid'] for result in results]
        if not results:
            logging.info('No more results, breaking out of loop')  # Log the end condition
            break
        logging.info(f'Fetching data for {len(pmids)} PMIDs from DynamoDB')  # Log the number of PMIDs
        keys = [{'pmid': pmid} for pmid in pmids]
        response = client.batch_get_item(RequestItems={'PubMedArticle': {'Keys': keys}})
        items = response['Responses']['PubMedArticle']
        for item in items:
            abstract = get_abstract(item)
            with open('abstract.csv', 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='\t')  # specify tab delimiter here
                writer.writerow([item['pmid'], abstract])
        offset += limit

def load_abstract(mysql_cursor):
    cwd = os.getcwd()
    load_abstract_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/abstract.csv' INTO TABLE reporting_abstracts FIELDS TERMINATED BY '\t' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES (pmid,abstract);"
    )
    mysql_cursor.execute(load_abstract_query)
    print(time.ctime() + "--" + ".csv file loaded")
    update_query = (
        "UPDATE reporting_abstracts SET abstractVarchar = CAST(abstract AS CHAR(15000)) where abstractVarchar is null;"
    )
    mysql_cursor.execute(update_query)
    print(time.ctime() + " reporting_abstract table updated with varchar equivalents")  

if __name__ == '__main__':
    # Create a MySQL connection to the Reciter database
    reciter_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
    with reciter_db.cursor() as reciter_db_cursor:
        load_abstract(reciter_db_cursor)
    # Close DB connection
    reciter_db.close()
