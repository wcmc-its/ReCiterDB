import json
import os
import time
import requests
import urllib.request
import logging

logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

import pymysql.cursors
import pymysql.err
from http.client import responses

def connect_mysql_server(username, db_password, db_hostname, database_name):
    """Establish a connection to MySQL or MariaDB server. This function is
    dependent on the PyMySQL library.
    See: https://github.com/PyMySQL/PyMySQL

    Args:
        username (string): username of the database user.
        password (string): password of the database user.
        db_hostname (string): hostname or IP address of the database server.
        database_name (string): the name of the database we are connecting to.

    Returns:
        MySQLConnection object.
    """

    try:
        mysql_db = pymysql.connect(user=DB_USERNAME,
                                   password=DB_PASSWORD,
                                   database=DB_NAME,
                                   host=DB_HOST)

        print("Connected to database server: " + DB_HOST,
                "; database: " + DB_NAME,
                "; with user: " + DB_USERNAME)

        return mysql_db

    except pymysql.err.MySQLError as err:
        print(time.ctime() + "--" + "Error connecting to the database. %s" % (err))


def get_person_article_pmid(mysql_cursor):
    """Looks up a list to PMIDs from the
       """ + DB_NAME + """.person_article table

    Args:
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.

    Returns:
        pmid (list): List of all rows in the pmid column.
    """
    get_metadata_query = (
        """
        SELECT distinct
            CAST(pmid as char) as pmid
        FROM """ + DB_NAME + """.person_article
        """
    )

    mysql_cursor.execute(get_metadata_query)

    pmid = list()

    for rec in mysql_cursor:
        pmid.append(rec[0])

    return pmid


def create_nih_API_url(article_pmid_list):
    """Create NIH RCR API URL by combining the
    base API and fields of intrest. API
    documentation can be found at:
    https://icite.od.nih.gov/api

    Multi record. This function combines
    all PMIDs from the list in one long URL.

    Args:
        article_pmid (list of PMIDs): API pmid.

    Returns:
        string: Full API URL of the record.

    Here's a sample JSON record: https://icite.od.nih.gov/api/pubs?pmids=19393196
    """

    API_BASE_URL = "https://icite.od.nih.gov/api/pubs?pmids="

    combined_pmids = ",".join(article_pmid_list)

    full_api_url = API_BASE_URL + combined_pmids

    return full_api_url


def truncate_analysis_nih(mysql_cursor):
    """This function will delete all rows in the
    analysis_nih table when called.

    Args:
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.
    """

    truncate_nih_query = (
        """
        truncate analysis_nih;
        """
    )

    mysql_cursor.execute(truncate_nih_query)
    print(time.ctime() + "--" + "Existing analysis_nih table truncated.")


def truncate_analysis_nih_cites(mysql_cursor):
    """This function will delete all rows in the
    analysis_nih_cites table when called.

    Args:
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.
    """

    truncate_nih_query = (
        """
        truncate analysis_nih_cites;
        """
    )

    mysql_cursor.execute(truncate_nih_query)
    print(time.ctime() + "--" + "Existing analysis_nih_cites table truncated.")


def truncate_analysis_nih_cites_clin(mysql_cursor):
    """This function will delete all rows in the
    analysis_nih_cites_clin table when called.

    Args:
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.
    """

    truncate_nih_query = (
        """
        truncate analysis_nih_cites_clin;
        """
    )

    mysql_cursor.execute(truncate_nih_query)
    print(time.ctime() + "--" + "Existing analysis_nih_cites_clin table truncated.")


def get_json_data(api_record_url):
    """Gets JSON data from API URL

    Args:
        api_record_url (string): URL of the API returning JSON data.

    Returns:
        dict: Python dictionary with JSON data or error.
    """
    try:
        api_request = urllib.request.urlopen(api_record_url)
        json_api_data = json.loads(api_request.read().decode())
        return json_api_data

    except urllib.error.URLError as err:
        print(time.ctime() + "--" + "%s--API URL: %s" % (err, api_record_url))
    except ValueError as err:
        print(time.ctime() + "--" + "Error parsing JSON data--Error %s" % (err))


def get_dict_value(dict_obj, *keys):
    """Gets the value of a key in a dictionary object.
        If the key is not found it returns None.

    Args:
        dict_obj (dict): Dictionary object to check.
        keys (string): Name of key or nested keys to search.

    Raises:
        AttributeError: If dictionary object is not passwed.

    Returns:
        Value of the key if found or None if not.
    """
    if not isinstance(dict_obj, dict):
        raise AttributeError("dict_obj needs to be of type dict.")

    dict_value = dict_obj

    for key in keys:
        try:
            dict_value = dict_value[key]
        except KeyError:
            return None
    return dict_value


def get_nih_records(nih_api_url):
    """Gets and returns API records from the NIH RCR URL.

    Args:
        nih_api_url (string): URL of the API record.

    Returns:
        list: NIH RCR API record list of tuples.
    """

    response = requests.get(nih_api_url)
    if response.status_code == 503:
        return None
    else:

        nih_record = get_json_data(nih_api_url)
    
        if isinstance(nih_record, dict):
            # We map dictionary value to each table column. This should
            # allow for easy update if the database tables or API records
            # change in the future.
            #
            # Because the API does not always return data for all keys,
            # we have to check each one with the get_dict_value function
            # and assign None (NULL) for the missing values.
            process_records = get_dict_value(nih_record, "data")
    
            records = []
    
            for record in process_records:
                new_record = (
                    get_dict_value(record, "pmid"),
                    get_dict_value(record, "year"),
                    get_dict_value(record, "is_research_article"),
                    get_dict_value(record, "is_clinical"),
                    get_dict_value(record, "relative_citation_ratio"),
                    get_dict_value(record, "nih_percentile"),
                    get_dict_value(record, "citation_count"),
                    get_dict_value(record, "citations_per_year"),
                    get_dict_value(record, "expected_citations_per_year"),
                    get_dict_value(record, "field_citation_rate"),
                    get_dict_value(record, "provisional"),
                    get_dict_value(record, "doi"),
                    get_dict_value(record, "human"),
                    get_dict_value(record, "animal"),
                    get_dict_value(record, "molecular_cellular"),
                    get_dict_value(record, "apt"),
                    get_dict_value(record, "x_coord"),
                    get_dict_value(record, "y_coord"),
                    get_dict_value(record, "cited_by_clin"),
                    get_dict_value(record, "cited_by"),
                    get_dict_value(record, "references")
                )
    
                records.append(new_record)
    
            print(time.ctime() + "--" + "New records retrived--API URL: %s" % (nih_api_url))
            return records
    
        return "Invalid record obtained from API URL"

def insert_analysis_nih(mysql_db, mysql_cursor, db_records):
    """Inserts the API records in the MySQL analysis_nih database table.

    Args:
        mysql_db (MySQLConnection object): The MySQL database where the table resides.
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.
        db_records (list): List of rows to be added to the database.

    Returns:
        string: Records added successfully or error message.
    """

    add_to_nih_table = (
        """
        INSERT INTO """ + DB_NAME + """.analysis_nih(
           `pmid`,
           `year`,
           `is_research_article`,
           `is_clinical`,
           `relative_citation_ratio`,
           `nih_percentile`,
           `citation_count`,
           `citations_per_year`,
           `expected_citations_per_year`,
           `field_citation_rate`,
           `provisional`,
           `doi`,
           `human`,
           `animal`,
           `molecular_cellular`,
           `apt`,
           `x_coord`,
           `y_coord`
        )
        VALUES(
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s
        )
        """
    )

    try:
        mysql_cursor.executemany(add_to_nih_table, db_records)
        mysql_db.commit()

        print(time.ctime() + "--" + "%s records successfully added to the database."
            % (len(db_records)))

    except pymysql.err.MySQLError as err:
        print(time.ctime() + "--" + "Error writing the records to the database. %s" % (err))


def insert_analysis_nih_cites(mysql_db, mysql_cursor, db_records):
    """Inserts the API records in the MySQL analysis_nih_cites_clin database table.

    Args:
        mysql_db (MySQLConnection object): The MySQL database where the table resides.
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.
        db_records (list): List of rows to be added to the database.

    Returns:
        string: Records added successfully or error message.
    """

    add_to_nih_table = (
        """
        INSERT INTO """ + DB_NAME + """.analysis_nih_cites(
           `citing_pmid`,
           `cited_pmid`
        )
        VALUES(
            %s, %s
        )
        """
    )

    try:
        mysql_cursor.executemany(add_to_nih_table, db_records)
        mysql_db.commit()

        print(time.ctime() + "--" + "%s records successfully added to the database."
            % (len(db_records)))

    except pymysql.err.MySQLError as err:
        print(time.ctime() + "--" + "Error writing the records to the database. %s" % (err))


def insert_analysis_nih_cites_clin(mysql_db, mysql_cursor, db_records):
    """Inserts the API records in the MySQL analysis_nih_cites_clin database table.

    Args:
        mysql_db (MySQLConnection object): The MySQL database where the table resides.
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.
        db_records (list): List of rows to be added to the database.

    Returns:
        string: Records added successfully or error message.
    """

    add_to_nih_table = (
        """
        INSERT INTO """ + DB_NAME + """.analysis_nih_cites_clin(
           `citing_pmid`,
           `cited_pmid`
        )
        VALUES(
            %s, %s
        )
        """
    )

    # Log the records that are going to be inserted
    for i, record in enumerate(db_records, start=1):
        logging.debug("Record %d: %s", i, record)

    try:
        mysql_cursor.executemany(add_to_nih_table, db_records)
        mysql_db.commit()

        print(time.ctime() + "--" + "%s records successfully added to the database."
            % (len(db_records)))

    except pymysql.err.MySQLError as err:
        logging.error("An error occurred while inserting records: %s", err)
        print(time.ctime() + "--" + "Error writing the records to the database. %s" % (err))


#########


if __name__ == '__main__':
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')

    # Create a MySQL connection to the Reciter database
    reciter_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
    reciter_db_cursor = reciter_db.cursor()

    # Truncate the analysis_nih, analysis_nih_cites, and analysis_nih_cites_clin
    # tables to remove all old records.
    truncate_analysis_nih(reciter_db_cursor)
    truncate_analysis_nih_cites(reciter_db_cursor)
    truncate_analysis_nih_cites_clin(reciter_db_cursor)

    # Get the PMIDs from the person_article table
    person_article_pmid = get_person_article_pmid(reciter_db_cursor)

    # Fibonacci sequence for retries
    fibonacci_sequence = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144]

    try:
        for i in range(0, len(person_article_pmid), 900):

            # Create API URL
            api_url = create_nih_API_url(person_article_pmid[i:i+900])
        
            # Initialize counters for while loop
            retries = 0
            success = False
            while retries < 10:  # try to get data up to 10 times
                # Get records from API
                nih_records = get_nih_records(api_url)
                    
                if nih_records is not None and not isinstance(nih_records, str):
                    success = True  # API call successful, exit the loop
                    
                    # Initialize the record lists here
                    analysis_nih_rec = []
                    analysis_nih_cites_rec = []
                    analysis_nih_cites_clin = []
                    
                    for item in nih_records:
                        # Get the records needed for the analysis_nih table
                        analysis_nih_rec.append(item[:18])
    
                        # Check if item has enough elements
                        if len(item) == 21:
                            # Process cited_by_clin
                            if item[18] is not None:
                                for cited_by_clin_item in item[18]:
                                    cited_by_clin = (cited_by_clin_item, item[0])
                                    analysis_nih_cites_clin.append(cited_by_clin)
    
                            # Process cited_by for the analysis_nih_cites table
                            if item[19] is not None:
                                for cited_by_item in item[19]:
                                    cited_by = (cited_by_item, item[0])
                                    analysis_nih_cites_rec.append(cited_by)
    
                            # Process references for the analysis_nih_cites table
                            if item[20] is not None:
                                for references_item in item[20]:
                                    references = (references_item, item[0])
                                    analysis_nih_cites_rec.append(references)
        
                    logger.info('Data queued for import: %s', analysis_nih_rec)
    
                    # Insert current records to the database
                    insert_analysis_nih(reciter_db, reciter_db_cursor, analysis_nih_rec)
                    insert_analysis_nih_cites(reciter_db, reciter_db_cursor, analysis_nih_cites_rec)
                    insert_analysis_nih_cites_clin(reciter_db, reciter_db_cursor, analysis_nih_cites_clin)
            
                    # Clear the processed records from memory
                    nih_records.clear()
                    analysis_nih_rec.clear()
                    analysis_nih_cites_rec.clear()
                    analysis_nih_cites_clin.clear()
                        
                    # Pause for 1 second between API calls
                    time.sleep(1)
    
                    # Exit the loop after successful call
                    break
    
                else:
                    retries += 1
                    print(f'API call failed with status code 503: {responses[503]}. Retry attempt {retries}.')
                    time.sleep(fibonacci_sequence[retries])  # wait for an increasing delay before next attempt
        
            # Ensure we haven't exceeded max retries
            if retries == 10:
                print('Max retry attempts exceeded. Please check the API service.')
                # handle this event: e.g. break, continue, or sys.exit()

    except RuntimeError as e:
        logger.error(f"Error occurred when fetching data: {str(e)}")

    except pymysql.err.OperationalError as e:
        if "MySQL server has gone away" in str(e):
            logger.error("MySQL connection lost. Re-establishing connection.")
            reciter_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
            reciter_db_cursor = reciter_db.cursor()
            # Optionally, you might want to retry the operation that failed here
        else:
            raise e

    except Exception as e:
        logger.error(f"An unexpected error occurred: {str(e)}")

    # Close DB connection
    reciter_db.close()
    reciter_db_cursor.close()
