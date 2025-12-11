import json
import os
import time
import urllib.request
import logging
import pymysql.cursors
import pymysql.err


# Configure logging to output to stdout
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(message)s')

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

def truncate_altmetric(mysql_cursor):
    """This function will delete all rows in the altmetric table when called.

    Args:
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.
    """
    truncate_altmetric_query = (
        """
        truncate altmetric;
        """
    )

    mysql_cursor.execute(truncate_altmetric_query)
    print(time.ctime() + "--" + "Existing altmetric table truncated.")

def get_altmetric_doi(mysql_cursor):
    """Gets the doi column from the altmetric table

    Args:
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.

    Returns:
        doi (list): List of all rows in the doi column.
    """
    get_metadata_query = (
        """
        SELECT doi FROM """ + DB_NAME + """.altmetric
        """
    )

    mysql_cursor.execute(get_metadata_query)

    doi = list()

    for rec in mysql_cursor:
        doi.append(rec[0])

    return doi

def get_person_article_doi(mysql_cursor):
    """Gets the doi column from the person_article table

    Args:
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.

    Returns:
        (list): List of all rows in the doi column.
    """
    get_metadata_query = (
        """
        select distinct
            doi,
            pmid,
            datePublicationAddedToEntrez
        from """ + DB_NAME + """.person_article
            where userAssertion = 'ACCEPTED'
                and doi is not null
                and round((UNIX_TIMESTAMP() - UNIX_TIMESTAMP(
                                                STR_TO_DATE(datePublicationAddedToEntrez,
                                                    '%Y-%m-%d'))) / (60*60*24),0)  < 730
        """
    )

    mysql_cursor.execute(get_metadata_query)

    doi = list()

    for rec in mysql_cursor:
        doi.append(rec[0])

    return doi

def create_article_url(article_doi):
    """Create altmetric API URL by combining the base API and doi.

    Args:
        article_doi (string): API doi field.

    Returns:
        string: Full API URL of the record.
    """
    API_BASE_URL = "https://api.altmetric.com/v1/doi/"

    api_url = list()

    for doi in article_doi:
        if doi != "":
            api_url.append(API_BASE_URL + doi)

    return api_url

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

def get_altmetric_record(api_url):
    """Gets and returns API record from the altmetric URL.

    Args:
        api_url (string): URL of the API record.

    Returns:
        tuple: Altmetric API record.
    """

    altmetric_record = get_json_data(api_url)

    if isinstance(altmetric_record, dict):
        # We map dictionary value to each table column. This should
        # allow for easy update if the database tables or API records
        # change in the future.
        #
        # Because the API does not always return data for all keys,
        # we have to check each one with the get_dict_value function
        # and assign None (NULL) for the missing values.

        new_record = (get_dict_value(altmetric_record, "doi"),
                      get_dict_value(altmetric_record, "pmid"),
                      get_dict_value(altmetric_record, "altmetric_jid"),
                      get_dict_value(altmetric_record, "context", "all", "count"),
                      get_dict_value(altmetric_record, "context", "all", "mean"),
                      get_dict_value(altmetric_record, "context", "all", "rank"),
                      get_dict_value(altmetric_record, "context", "all", "pct"),
                      get_dict_value(altmetric_record,
                                    "context",
                                    "all",
                                    "higher_than"),
                      get_dict_value(altmetric_record,
                                    "context",
                                    "similar_age_3m",
                                    "count"),
                      get_dict_value(altmetric_record,
                                    "context",
                                    "similar_age_3m",
                                    "mean"),
                      get_dict_value(altmetric_record,
                                    "context",
                                    "similar_age_3m",
                                    "rank"),
                      get_dict_value(altmetric_record,
                                    "context",
                                    "similar_age_3m",
                                    "pct"),
                      get_dict_value(altmetric_record,
                                    "context",
                                    "similar_age_3m",
                                    "higher_than"),
                      get_dict_value(altmetric_record, "altmetric_id"),
                      get_dict_value(altmetric_record, "cited_by_msm_count"),
                      get_dict_value(altmetric_record, "cited_by_posts_count"),
                      get_dict_value(altmetric_record, "cited_by_tweeters_count"),
                      get_dict_value(altmetric_record, "cited_by_feeds_count"),
                      get_dict_value(altmetric_record, "cited_by_fbwalls_count"),
                      get_dict_value(altmetric_record, "cited_by_rh_count"),
                      get_dict_value(altmetric_record, "cited_by_accounts_count"),
                      get_dict_value(altmetric_record, "last_updated"),
                      get_dict_value(altmetric_record, "score"),
                      get_dict_value(altmetric_record, "history", "1y"),
                      get_dict_value(altmetric_record, "history", "6m"),
                      get_dict_value(altmetric_record, "history", "3m"),
                      get_dict_value(altmetric_record, "history", "1m"),
                      get_dict_value(altmetric_record, "history", "1w"),
                      get_dict_value(altmetric_record, "history", "at"),
                      get_dict_value(altmetric_record, "added_on"),
                      get_dict_value(altmetric_record, "published_on"),
                      get_dict_value(altmetric_record, "readers", "mendeley"))

        print(time.ctime() + "--" + "New record created--API URL: %s" % (api_url))
        return new_record

    return "Invalid record obtained from API URL"


def insert_altmetric_records(mysql_db, mysql_cursor, db_records):
    """Inserts the API record in the MySQL database table.

    Args:
        mysql_db (MySQLConnection object): The MySQL database where the table resides.
        mysql_cursor (CMySQLCursor): Executes an SQL query against the database.
        db_records (list): List of rows to be added to the database.

    Returns:
        string: Records added successfully or error message.
    """

    add_to_altmetric_table = (
        """
        INSERT INTO """ + DB_NAME + """.altmetric(
            `doi`,
            `pmid`,
            `altmetric_jid`,
            `context-all-count`,
            `context-all-mean`,
            `context-all-rank`,
            `context-all-pct`,
            `context-all-higher_than`,
            `context-similar_age_3m-count`,
            `context-similar_age_3m-mean`,
            `context-similar_age_3m-rank`,
            `context-similar_age_3m-pct`,
            `context-similar_age_3m-higher_than`,
            `altmetric_id`,
            `cited_by_msm_count`,
            `cited_by_posts_count`,
            `cited_by_tweeters_count`,
            `cited_by_feeds_count`,
            `cited_by_fbwalls_count`,
            `cited_by_rh_count`,
            `cited_by_accounts_count`,
            `last_updated`,
            `score`,
            `history-1y`,
            `history-6m`,
            `history-3m`,
            `history-1m`,
            `history-1w`,
            `history-at`,
            `added_on`,
            `published_on`,
            `readers-mendeley`
        )
        VALUES(
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        """
    )

    try:
        mysql_cursor.executemany(add_to_altmetric_table, db_records)
        mysql_db.commit()

        print(time.ctime() + "--" + "%s records successfully added to the database."
            % (len(db_records)))

    except pymysql.err.MySQLError as err:
        print(time.ctime() + "--" + "Error writing the records to the database. %s" % (err))


if __name__ == '__main__':
	
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')

    # Create a connection to get metadata from the
    # person_article table. This data is used to
    # generate the API URL.
    person_article_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
    person_article_cursor = person_article_db.cursor()

    # Create a connection to use for inserting records
    # In the altmetric table. This is the API response
    # we need to populate in the altmetric table.
    altmetric_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
    altmetric_cursor = altmetric_db.cursor()

    person_article_doi = get_person_article_doi(person_article_cursor)
    altmetric_doi = get_altmetric_doi(altmetric_cursor)

    # Convert the DOI column to lowercase. In some entries the DOI is
    # capitalized and that causes problems with comparing the list.
    # the API returns lowecase, so they should be lower.
    url_doi = map(str.lower, person_article_doi)

    print(time.ctime() + "--" + "Processing %s URLs" % (len(person_article_doi)))

    article_api_url = create_article_url(url_doi)

    altmetric_records = []

    for url in article_api_url:
        record = get_altmetric_record(url)

        if isinstance(record, tuple):
            altmetric_records.append(record)

        # Pause for 1 second to comply with
        # Altmetric free API service.
        time.sleep(1)

    # Truncate the altmetric table to remove all old records.
    truncate_altmetric(altmetric_cursor)
    insert_altmetric_records(altmetric_db, altmetric_cursor, altmetric_records)

    person_article_db.close()
    person_article_cursor.close()
    altmetric_db.close()
    altmetric_cursor.close()