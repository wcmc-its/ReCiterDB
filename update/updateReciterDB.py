import json
import os
import time
import urllib.request

import pymysql.cursors
import pymysql.err

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
                                   host=DB_HOST,
                                   autocommit=True,
                                   local_infile=True)

        print("Connected to database server: " + DB_HOST,
                "; database: " + DB_NAME,
                "; with user: " + DB_USERNAME)

        return mysql_db

    except pymysql.err.MySQLError as err:
        print(time.ctime() + "--" + "Error connecting to the database. %s" % (err))


def truncate_person(mysql_cursor):
    truncate_person_query = (
        """        
        truncate person;      
        """
    )
    mysql_cursor.execute(truncate_person_query)
    print(time.ctime() + "--" + "person table truncated")


def truncate_person_article(mysql_cursor):
    truncate_person_article_query = (
        """        
        truncate person_article;      
        """
    )
    mysql_cursor.execute(truncate_person_article_query)
    print(time.ctime() + "--" + "person_article table truncated")



def truncate_person_article_author(mysql_cursor):
    truncate_person_article_author_query = (
        """        
        truncate person_article_author;      
        """
    )
    mysql_cursor.execute(truncate_person_article_author_query)
    print(time.ctime() + "--" + "person_article_author table truncated")

def truncate_person_article_department(mysql_cursor):
    truncate_person_article_department_query = (
        """        
        truncate person_article_department;      
        """
    )
    mysql_cursor.execute(truncate_person_article_department_query)
    print(time.ctime() + "--" + "person_article_department table truncated")



def truncate_person_article_grant(mysql_cursor):
    truncate_person_article_grant_query = (
        """        
        truncate person_article_grant;      
        """
    )
    mysql_cursor.execute(truncate_person_article_grant_query)
    print(time.ctime() + "--" + "person_article_grant table truncated")



def truncate_person_article_keyword(mysql_cursor):
    truncate_person_article_keyword_query = (
        """        
        truncate person_article_keyword;      
        """
    )
    mysql_cursor.execute(truncate_person_article_keyword_query)
    print(time.ctime() + "--" + "person_article_keyword table truncated")



def truncate_person_article_relationship(mysql_cursor):
    truncate_person_article_relationship_query = (
        """        
        truncate person_article_relationship;      
        """
    )
    mysql_cursor.execute(truncate_person_article_relationship_query)
    print(time.ctime() + "--" + "person_article_relationship table truncated")



def truncate_person_article_scopus_non_target_author_affiliation(mysql_cursor):
    truncate_person_article_scopus_non_target_author_affiliation_query = (
        """        
        truncate person_article_scopus_non_target_author_affiliation;      
        """
    )
    mysql_cursor.execute(truncate_person_article_scopus_non_target_author_affiliation_query)
    print(time.ctime() + "--" + "person_article_scopus_non_target_authorship_affiliation table truncated")



def truncate_person_article_scopus_target_author_affiliation(mysql_cursor):
    truncate_person_article_scopus_target_author_affiliation_query = (
        """        
        truncate person_article_scopus_target_author_affiliation;      
        """
    )
    mysql_cursor.execute(truncate_person_article_scopus_target_author_affiliation_query)
    print(time.ctime() + "--" + "person_article_scopus_target_authorship_affiliation table truncated")



def truncate_person_person_type(mysql_cursor):
    truncate_person_person_type_query = (
        """        
        truncate person_person_type;      
        """
    )
    mysql_cursor.execute(truncate_person_person_type_query)
    print(time.ctime() + "--" + "person_person_person_type table truncated")



def load_person1(mysql_cursor):
    cwd = os.getcwd()
    load_person1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person1.csv' INTO TABLE person FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,dateAdded,dateUpdated,`precision`,recall,countSuggestedArticles,countPendingArticles,overallAccuracy,mode);"
    )
    mysql_cursor.execute(load_person1_query)
    print(time.ctime() + "--" + "person1.csv file loaded")



def load_person2(mysql_cursor):
    cwd = os.getcwd()
    load_person2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person2.csv' INTO TABLE person FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,dateAdded,dateUpdated,`precision`,recall,countSuggestedArticles,countPendingArticles,overallAccuracy,mode);"
    )
    mysql_cursor.execute(load_person2_query)
    print(time.ctime() + "--" + "person2.csv file loaded")



def load_person_article1(mysql_cursor):
    cwd = os.getcwd()
    load_person_article1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article1.csv' INTO TABLE person_article FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,pmid,pmcid,totalArticleScoreStandardized,totalArticleScoreNonStandardized,userAssertion,publicationDateDisplay,publicationDateStandardized,publicationTypeCanonical,scopusDocID,journalTitleVerbose,articleTitle,feedbackScoreAccepted,feedbackScoreRejected,feedbackScoreNull,articleAuthorNameFirstName,articleAuthorNameLastName,institutionalAuthorNameFirstName,institutionalAuthorNameMiddleName,institutionalAuthorNameLastName,nameMatchFirstScore,nameMatchFirstType,nameMatchMiddleScore,nameMatchMiddleType,nameMatchLastScore,nameMatchLastType,nameMatchModifierScore,nameScoreTotal,emailMatch,emailMatchScore,journalSubfieldScienceMetrixLabel,journalSubfieldScienceMetrixID,journalSubfieldDepartment,journalSubfieldScore,relationshipEvidenceTotalScore,relationshipMinimumTotalScore,relationshipNonMatchCount,relationshipNonMatchScore,articleYear,identityBachelorYear,discrepancyDegreeYearBachelor,discrepancyDegreeYearBachelorScore,identityDoctoralYear,discrepancyDegreeYearDoctoral,discrepancyDegreeYearDoctoralScore,genderScoreArticle,genderScoreIdentity,genderScoreIdentityArticleDiscrepancy,personType,personTypeScore,countArticlesRetrieved,articleCountScore,targetAuthorInstitutionalAffiliationArticlePubmedLabel,pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore,scopusNonTargetAuthorInstitutionalAffiliationSource,scopusNonTargetAuthorInstitutionalAffiliationScore,totalArticleScoreWithoutClustering,clusterScoreAverage,clusterReliabilityScore,clusterScoreModificationOfTotalScore,datePublicationAddedToEntrez,clusterIdentifier,doi,issn,issue,journalTitleISOabbreviation,pages,timesCited,volume);"
    )
    mysql_cursor.execute(load_person_article1_query)
    print(time.ctime() + "--" + "person_article1.csv file loaded")



def load_person_article2(mysql_cursor):
    cwd = os.getcwd()
    load_person_article2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article2.csv' INTO TABLE person_article FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,pmid,pmcid,totalArticleScoreStandardized,totalArticleScoreNonStandardized,userAssertion,publicationDateDisplay,publicationDateStandardized,publicationTypeCanonical,scopusDocID,journalTitleVerbose,articleTitle,feedbackScoreAccepted,feedbackScoreRejected,feedbackScoreNull,articleAuthorNameFirstName,articleAuthorNameLastName,institutionalAuthorNameFirstName,institutionalAuthorNameMiddleName,institutionalAuthorNameLastName,nameMatchFirstScore,nameMatchFirstType,nameMatchMiddleScore,nameMatchMiddleType,nameMatchLastScore,nameMatchLastType,nameMatchModifierScore,nameScoreTotal,emailMatch,emailMatchScore,journalSubfieldScienceMetrixLabel,journalSubfieldScienceMetrixID,journalSubfieldDepartment,journalSubfieldScore,relationshipEvidenceTotalScore,relationshipMinimumTotalScore,relationshipNonMatchCount,relationshipNonMatchScore,articleYear,identityBachelorYear,discrepancyDegreeYearBachelor,discrepancyDegreeYearBachelorScore,identityDoctoralYear,discrepancyDegreeYearDoctoral,discrepancyDegreeYearDoctoralScore,genderScoreArticle,genderScoreIdentity,genderScoreIdentityArticleDiscrepancy,personType,personTypeScore,countArticlesRetrieved,articleCountScore,targetAuthorInstitutionalAffiliationArticlePubmedLabel,pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore,scopusNonTargetAuthorInstitutionalAffiliationSource,scopusNonTargetAuthorInstitutionalAffiliationScore,totalArticleScoreWithoutClustering,clusterScoreAverage,clusterReliabilityScore,clusterScoreModificationOfTotalScore,datePublicationAddedToEntrez,clusterIdentifier,doi,issn,issue,journalTitleISOabbreviation,pages,timesCited,volume);"
    )
    mysql_cursor.execute(load_person_article2_query)
    print(time.ctime() + "--" + "person_article2.csv file loaded")



def load_person_article_author1(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_author1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_author1.csv' INTO TABLE person_article_author FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,pmid,authorFirstName,authorLastName,targetAuthor,rank,orcid,equalContrib);"
    )
    mysql_cursor.execute(load_person_article_author1_query)
    print(time.ctime() + "--" + "person_article_author1.csv file loaded")



def load_person_article_author2(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_author2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_author2.csv' INTO TABLE person_article_author FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,pmid,authorFirstName,authorLastName,targetAuthor,rank,orcid,equalContrib);"
    )
    mysql_cursor.execute(load_person_article_author2_query)
    print(time.ctime() + "--" + "person_article_author2.csv file loaded")



def load_person_article_department1(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_department1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_department1.csv' INTO TABLE person_article_department FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,pmid,identityOrganizationalUnit,articleAffiliation,organizationalUnitType,organizationalUnitMatchingScore,organizationalUnitModifier,organizationalUnitModifierScore);"
    )
    mysql_cursor.execute(load_person_article_department1_query)
    print(time.ctime() + "--" + "person_article_department1.csv file loaded")



def load_person_article_department2(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_department2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_department2.csv' INTO TABLE person_article_department FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,pmid,identityOrganizationalUnit,articleAffiliation,organizationalUnitType,organizationalUnitMatchingScore,organizationalUnitModifier,organizationalUnitModifierScore);"
    )
    mysql_cursor.execute(load_person_article_department2_query)
    print(time.ctime() + "--" + "person_article_department2.csv file loaded")



def load_person_article_grant1(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_grant1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_grant1.csv' INTO TABLE person_article_grant FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,pmid,articleGrant,grantMatchScore,institutionGrant);"
    )
    mysql_cursor.execute(load_person_article_grant1_query)
    print(time.ctime() + "--" + "person_article_grant1.csv file loaded")



def load_person_article_grant2(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_grant2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_grant2.csv' INTO TABLE person_article_grant FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,pmid,articleGrant,grantMatchScore,institutionGrant);"
    )
    mysql_cursor.execute(load_person_article_grant2_query)
    print(time.ctime() + "--" + "person_article_grant2.csv file loaded")



def load_person_article_keyword1(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_keyword1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_keyword1.csv' INTO TABLE person_article_keyword FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,pmid,keyword);"
    )
    mysql_cursor.execute(load_person_article_keyword1_query)
    print(time.ctime() + "--" + "person_article_keyword1.csv file loaded")



def load_person_article_keyword2(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_keyword2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_keyword2.csv' INTO TABLE person_article_keyword FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,pmid,keyword);"
    )
    mysql_cursor.execute(load_person_article_keyword2_query)
    print(time.ctime() + "--" + "person_article_keyword2.csv file loaded")



def load_person_article_relationship1(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_relationship1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_relationship1.csv' INTO TABLE person_article_relationship FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,pmid,relationshipNameArticleFirstName,relationshipNameArticleLastName,relationshipNameIdentityFirstName,relationshipNameIdentityLastName,relationshipType,relationshipMatchType,relationshipMatchingScore,relationshipVerboseMatchModifierScore,relationshipMatchModifierMentor,relationshipMatchModifierMentorSeniorAuthor,relationshipMatchModifierManager,relationshipMatchModifierManagerSeniorAuthor);"
    )
    mysql_cursor.execute(load_person_article_relationship1_query)
    print(time.ctime() + "--" + "person_article_relationship1.csv file loaded")



def load_person_article_relationship2(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_relationship2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_relationship2.csv' INTO TABLE person_article_relationship FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,pmid,relationshipNameArticleFirstName,relationshipNameArticleLastName,relationshipNameIdentityFirstName,relationshipNameIdentityLastName,relationshipType,relationshipMatchType,relationshipMatchingScore,relationshipVerboseMatchModifierScore,relationshipMatchModifierMentor,relationshipMatchModifierMentorSeniorAuthor,relationshipMatchModifierManager,relationshipMatchModifierManagerSeniorAuthor);"
    )
    mysql_cursor.execute(load_person_article_relationship2_query)
    print(time.ctime() + "--" + "person_article_relationship2.csv file loaded")



def load_person_article_scopus_non_target_author_affiliation1(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_scopus_non_target_author_affiliation1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_scopus_non_target_author_affiliation1.csv' INTO TABLE person_article_scopus_non_target_author_affiliation FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,pmid,nonTargetAuthorInstitutionLabel,nonTargetAuthorInstitutionID,nonTargetAuthorInstitutionCount);"
    )
    mysql_cursor.execute(load_person_article_scopus_non_target_author_affiliation1_query)
    print(time.ctime() + "--" + "person_article_scopus_non_target_author_affiliation1.csv file loaded")



def load_person_article_scopus_non_target_author_affiliation2(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_scopus_non_target_author_affiliation2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_scopus_non_target_author_affiliation2.csv' INTO TABLE person_article_scopus_non_target_author_affiliation FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,pmid,nonTargetAuthorInstitutionLabel,nonTargetAuthorInstitutionID,nonTargetAuthorInstitutionCount);"
    )
    mysql_cursor.execute(load_person_article_scopus_non_target_author_affiliation2_query)
    print(time.ctime() + "--" + "person_article_scopus_non_target_author_affiliation2.csv file loaded")



def load_person_article_scopus_target_author_affiliation1(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_scopus_target_author_affiliation1_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_scopus_target_author_affiliation1.csv' INTO TABLE person_article_scopus_target_author_affiliation FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES (personIdentifier,pmid,targetAuthorInstitutionalAffiliationSource,scopusTargetAuthorInstitutionalAffiliationIdentity,targetAuthorInstitutionalAffiliationArticleScopusLabel,targetAuthorInstitutionalAffiliationArticleScopusAffiliationId,targetAuthorInstitutionalAffiliationMatchType,targetAuthorInstitutionalAffiliationMatchTypeScore);"
    )
    mysql_cursor.execute(load_person_article_scopus_target_author_affiliation1_query)
    print(time.ctime() + "--" + "person_article_scopus_target_author_affiliation1.csv file loaded")



def load_person_article_scopus_target_author_affiliation2(mysql_cursor):
    cwd = os.getcwd()
    load_person_article_scopus_target_author_affiliation2_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_article_scopus_target_author_affiliation2.csv' INTO TABLE person_article_scopus_target_author_affiliation FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 0 LINES (personIdentifier,pmid,targetAuthorInstitutionalAffiliationSource,scopusTargetAuthorInstitutionalAffiliationIdentity,targetAuthorInstitutionalAffiliationArticleScopusLabel,targetAuthorInstitutionalAffiliationArticleScopusAffiliationId,targetAuthorInstitutionalAffiliationMatchType,targetAuthorInstitutionalAffiliationMatchTypeScore);"
    )
    mysql_cursor.execute(load_person_article_scopus_target_author_affiliation2_query)
    print(time.ctime() + "--" + "person_article_scopus_target_author_affiliation2.csv file loaded")



def load_person_person_type(mysql_cursor):
    cwd = os.getcwd()
    load_person_person_type_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/person_person_type.csv' INTO TABLE person_person_type FIELDS TERMINATED BY ',' ENCLOSED BY '' IGNORE 0 LINES (personIdentifier,personType);"
    )
    mysql_cursor.execute(load_person_person_type_query)
    print(time.ctime() + "--" + "person_person_type.csv file loaded")



def create_identity_temp_table(mysql_cursor):
    create_identity_temp_table_query = (
        """        
        CREATE TABLE IF NOT EXISTS identity_temp (id int(11) NOT NULL AUTO_INCREMENT, personIdentifier varchar(128) COLLATE utf8mb4_unicode_ci NOT NULL, firstName varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL, middleName varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL, lastName varchar(128) COLLATE utf8mb4_unicode_ci DEFAULT NULL, title varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL, primaryEmail varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL, primaryOrganizationalUnit varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL, primaryInstitution varchar(200) COLLATE utf8mb4_unicode_ci DEFAULT NULL, PRIMARY KEY (id), KEY id (personIdentifier) USING BTREE ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;               
        """
    )
    mysql_cursor.execute(create_identity_temp_table_query)
    print(time.ctime() + "--" + "identity_temp table created")



def load_identity(mysql_cursor):
    cwd = os.getcwd()
    load_identity_query = (
        "LOAD DATA LOCAL INFILE '" + cwd + "/temp/parsedOutput/identity.csv' INTO TABLE identity_temp FIELDS TERMINATED BY '\t' ENCLOSED BY '\"'  LINES TERMINATED BY '\n' IGNORE 0 LINES (personIdentifier,title,firstName,middleName,lastName,primaryEmail,primaryOrganizationalUnit,primaryInstitution);"
    )
    mysql_cursor.execute(load_identity_query)
    print(time.ctime() + "--" + "identity.csv file loaded")



def update_person(mysql_cursor):
    update_person_query = (
        """        
        UPDATE person p JOIN identity_temp i on i.personIdentifier = p.personIdentifier SET p.firstName = i.firstName, p.middleName = i.middleName, p.lastName = i.lastName, p.title = i.title, p.primaryEmail = i.primaryEmail, p.primaryOrganizationalUnit = i.primaryOrganizationalUnit, p.primaryInstitution = i.primaryInstitution;                 
        """
    )
    mysql_cursor.execute(update_person_query)
    print(time.ctime() + "--" + "person table updated with data from identity_temp table")



def drop_identity_temp_table(mysql_cursor):
    drop_identity_temp_table_query = (
        """        
        DROP table identity_temp;        
        """
    )
    mysql_cursor.execute(drop_identity_temp_table_query)
    print(time.ctime() + "--" + "identity_temp table dropped")



if __name__ == '__main__':
    DB_USERNAME = os.getenv('DB_USERNAME')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')

    # Create a MySQL connection to the Reciter database
    reciter_db = connect_mysql_server(DB_USERNAME, DB_PASSWORD, DB_HOST, DB_NAME)
    reciter_db_cursor = reciter_db.cursor()

    # Do stuff!

    truncate_person(reciter_db_cursor)
    truncate_person_article(reciter_db_cursor)
    truncate_person_article_author(reciter_db_cursor)
    truncate_person_article_department(reciter_db_cursor)
    truncate_person_article_grant(reciter_db_cursor)
    truncate_person_article_keyword(reciter_db_cursor)
    truncate_person_article_relationship(reciter_db_cursor)
    truncate_person_article_scopus_non_target_author_affiliation(reciter_db_cursor)
    truncate_person_article_scopus_target_author_affiliation(reciter_db_cursor)
    truncate_person_person_type(reciter_db_cursor)
    load_person1(reciter_db_cursor)
    load_person2(reciter_db_cursor)
    load_person_article1(reciter_db_cursor)
    load_person_article2(reciter_db_cursor)
    load_person_article_author1(reciter_db_cursor)
    load_person_article_author2(reciter_db_cursor)
    load_person_article_department1(reciter_db_cursor)
    load_person_article_department2(reciter_db_cursor)
    load_person_article_grant1(reciter_db_cursor)
    load_person_article_grant2(reciter_db_cursor)
    load_person_article_keyword1(reciter_db_cursor)
    load_person_article_keyword2(reciter_db_cursor)
    load_person_article_relationship1(reciter_db_cursor)
    load_person_article_relationship2(reciter_db_cursor)
    load_person_article_scopus_non_target_author_affiliation1(reciter_db_cursor)
    load_person_article_scopus_non_target_author_affiliation2(reciter_db_cursor)
    load_person_article_scopus_target_author_affiliation1(reciter_db_cursor)
    load_person_article_scopus_target_author_affiliation2(reciter_db_cursor)
    load_person_person_type(reciter_db_cursor)
    create_identity_temp_table(reciter_db_cursor)
    load_identity(reciter_db_cursor)
    update_person(reciter_db_cursor)
    drop_identity_temp_table(reciter_db_cursor)


    # Close DB connection
    reciter_db.close()
    reciter_db_cursor.close()
