import json
import time
import os
import csv
from init import pymysql
import MySQLdb

import boto3
from boto3.dynamodb.conditions import Key, Attr
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')

def download_file(s3_client, bucket_name, object_key, local_file_path):
    s3_client.download_file(bucket_name, object_key, local_file_path)

def download_directory_from_s3(bucket_name, remote_directory_name, local_directory_name):
    s3_resource = boto3.resource('s3')
    s3_client = boto3.client('s3')
    bucket = s3_resource.Bucket(bucket_name)
    number = 0
    objects = list(bucket.objects.filter(Prefix=remote_directory_name))
    print(f"Total files to download: {len(objects)}")

    if not os.path.exists(local_directory_name):
        os.makedirs(local_directory_name)

    max_workers = 20  # Adjust this number based on your system's capacity

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_object = {}
        for obj in objects:
            filename = os.path.basename(obj.key)
            if not filename:
                continue  # Skip directories
            local_file_path = os.path.join(local_directory_name, filename)
            future = executor.submit(download_file, s3_client, bucket_name, obj.key, local_file_path)
            future_to_object[future] = obj.key

        for future in as_completed(future_to_object):
            obj_key = future_to_object[future]
            try:
                future.result()
                number += 1
                print(f"Downloaded: {obj_key}")
            except Exception as e:
                print(f"Error downloading {obj_key}: {e}")

    print(f"Downloaded {number} files from S3.")

def scan_table(table_name):
    # Record time for scanning the entire table
    print(dynamodb)
    start = time.time()
    table = dynamodb.Table(table_name)

    response = table.scan()
    items = response['Items']

    # Continue to get all records in the table, using ExclusiveStartKey
    while 'LastEvaluatedKey' in response:
        print(f"Retrieved {len(response['Items'])} items")
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])

    print('Execution time:', time.time() - start)
    return items

# Paths for data
originalDataPath = 'temp/s3Output/'
outputPath = 'temp/parsedOutput/'

# Ensure the output directories exist
if not os.path.exists(originalDataPath):
    os.makedirs(originalDataPath)

if not os.path.exists(outputPath):
    os.makedirs(outputPath)

# Flag to control whether to download data from S3
download_from_s3 = True  # Set to True to download data from S3, False to use existing data

if download_from_s3:
    # Download files from S3 using multithreading
    download_directory_from_s3('reciter-dynamodb', 'AnalysisOutput/', originalDataPath)
else:
    print("Skipping download from S3. Using existing data in local directory.")

# Call scan_table function for Identity table
identities = scan_table('Identity')
print("Count items from DynamoDB Identity table:", len(identities))

# Prepare list of files to process
person_list = os.listdir(originalDataPath)

# Remove any unwanted files, like ".DS_Store" or ".gitkeep"
unwanted_files = [".DS_Store", ".gitkeep"]
for unwanted_file in unwanted_files:
    try:
        person_list.remove(unwanted_file)
    except ValueError:
        pass  # File not in list; proceed

person_list.sort()
print(f"Processing {len(person_list)} files.")

# Read the files
items = []
for filename in person_list:
    start = time.time()
    file_path = os.path.join(originalDataPath, filename)
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            items.append(json.loads(line))
    print(f'Execution time for {filename}:', time.time() - start)
print(f"Total items loaded: {len(items)}")

# Prepare identity.csv
with open(os.path.join(outputPath, 'identity.csv'), 'w', encoding='utf-8') as f:
    count = 0
    for identity in identities:
        personIdentifier = identity.get('uid', '')
        identity_data = identity.get('identity', {})
        title = identity_data.get('title', '')
        primaryName = identity_data.get('primaryName', {})
        firstName = primaryName.get('firstName', '')
        middleName = primaryName.get('middleName', '')
        lastName = primaryName.get('lastName', '')
        primaryEmail = identity_data.get('primaryEmail', '')
        primaryOrganizationalUnit = identity_data.get('primaryOrganizationalUnit', '')
        primaryInstitution = identity_data.get('primaryInstitution', '')

        f.write(
            f"\"{personIdentifier}\"\t\"{title}\"\t\"{firstName}\"\t\"{middleName}\"\t\"{lastName}\"\t\"{primaryEmail}\"\t\"{primaryOrganizationalUnit}\"\t\"{primaryInstitution}\"\n"
        )
        count += 1
        print("Identities imported into temp table:", count)

f.close()

# Prepare person_person_type.csv
f = open(outputPath + 'person_person_type.csv', 'w', encoding='utf-8')
for identity in identities:
    a = identity.get('identity', {})
    personIdentifier = a.get('uid', '')
    personTypes = a.get('personTypes', [])
    for each_person_type in personTypes:
        f.write(str(personIdentifier) + "," + str(each_person_type) + "\n")
f.close()




# Open the CSV file for writing
with open(os.path.join(outputPath, 'person_article2.csv'), 'w', encoding='utf-8') as f:
    # Write column names into the file
    f.write(
        "personIdentifier," + "pmid," + "authorshipLikelihoodScore," + "pmcid," + "totalArticleScoreStandardized," +
        "totalArticleScoreNonStandardized," + "userAssertion," + "publicationDateDisplay," +
        "publicationDateStandardized," + "publicationTypeCanonical," + "scopusDocID," + "journalTitleVerbose," +
        "articleTitle," + "feedbackScoreAccepted," + "feedbackScoreRejected," + "feedbackScoreNull," +
        "articleAuthorNameFirstName," + "articleAuthorNameLastName," + "institutionalAuthorNameFirstName," +
        "institutionalAuthorNameMiddleName," + "institutionalAuthorNameLastName," + "nameMatchFirstScore," +
        "nameMatchFirstType," + "nameMatchMiddleScore," + "nameMatchMiddleType," + "nameMatchLastScore," +
        "nameMatchLastType," + "nameMatchModifierScore," + "nameScoreTotal," + "emailMatch," + "emailMatchScore," +
        "journalSubfieldScienceMetrixLabel," + "journalSubfieldScienceMetrixID," + "journalSubfieldDepartment," +
        "journalSubfieldScore," + "relationshipEvidenceTotalScore," + "relationshipMinimumTotalScore," +
        "relationshipNonMatchCount," + "relationshipNonMatchScore," + "articleYear," + "identityBachelorYear," +
        "discrepancyDegreeYearBachelor," + "discrepancyDegreeYearBachelorScore," + "identityDoctoralYear," +
        "discrepancyDegreeYearDoctoral," + "discrepancyDegreeYearDoctoralScore," + "genderScoreArticle," +
        "genderScoreIdentity," + "genderScoreIdentityArticleDiscrepancy," + "personType," + "personTypeScore," +
        "countArticlesRetrieved," + "articleCountScore," + "targetAuthorInstitutionalAffiliationArticlePubmedLabel," +
        "pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore," + "scopusNonTargetAuthorInstitutionalAffiliationSource," +
        "scopusNonTargetAuthorInstitutionalAffiliationScore," + "totalArticleScoreWithoutClustering," +
        "clusterScoreAverage," + "clusterReliabilityScore," + "clusterScoreModificationOfTotalScore," +
        "datePublicationAddedToEntrez," + "clusterIdentifier," + "doi," + "issn," + "issue," +
        "journalTitleISOabbreviation," + "pages," + "timesCited," + "volume," + "feedbackScoreCites," +
        "feedbackScoreCoAuthorName," + "feedbackScoreEmail," + "feedbackScoreInstitution," + "feedbackScoreJournal," +
        "feedbackScoreJournalSubField," + "feedbackScoreKeyword," + "feedbackScoreOrcid," + "feedbackScoreOrcidCoAuthor," +
        "feedbackScoreOrganization," + "feedbackScoreTargetAuthorName," + "feedbackScoreYear" + "\n"
    )

    count = 0
    for item in items:
        try:
            article_features = item.get('reCiterArticleFeatures', [])
            article_count = len(article_features)
        except KeyError as e:
            print(f"Error getting article features for person {item.get('personIdentifier', '')}: {e}")
            continue

        personIdentifier = item.get('personIdentifier', '')

        for article in article_features:
            try:
                pmid = article.get('pmid', '')
                pmcid = article.get('pmcid', '')

                # Determine if the record is in new or old format
                is_new_format = 'authorshipLikelihoodScore' in article

                # Initialize variables to default values
                authorshipLikelihoodScore = ''
                totalArticleScoreStandardized = ''
                totalArticleScoreNonStandardized = ''
                userAssertion = ''
                publicationDateStandardized = ''
                publicationDateDisplay = ''
                publicationTypeCanonical = ''
                scopusDocID = ''
                journalTitleVerbose = ''
                articleTitle = ''
                feedbackScoreAccepted = ''
                feedbackScoreRejected = ''
                feedbackScoreNull = ''
                articleAuthorName_firstName = ''
                articleAuthorName_lastName = ''
                institutionalAuthorName_firstName = ''
                institutionalAuthorName_middleName = ''
                institutionalAuthorName_lastName = ''
                nameMatchFirstScore = ''
                nameMatchFirstType = ''
                nameMatchMiddleScore = ''
                nameMatchMiddleType = ''
                nameMatchLastScore = ''
                nameMatchLastType = ''
                nameMatchModifierScore = ''
                nameScoreTotal = ''
                emailMatch = ''
                emailMatchScore = ''
                journalSubfieldScienceMetrixLabel = ''
                journalSubfieldScienceMetrixID = ''
                journalSubfieldDepartment = ''
                journalSubfieldScore = ''
                relationshipEvidenceTotalScore = ''
                relationshipMinimumTotalScore = ''
                relationshipNonMatchCount = ''
                relationshipNonMatchScore = ''
                articleYear = ''
                identityBachelorYear = ''
                discrepancyDegreeYearBachelor = ''
                discrepancyDegreeYearBachelorScore = ''
                identityDoctoralYear = ''
                discrepancyDegreeYearDoctoral = ''
                discrepancyDegreeYearDoctoralScore = ''
                genderScoreArticle = ''
                genderScoreIdentity = ''
                genderScoreIdentityArticleDiscrepancy = ''
                personType = ''
                personTypeScore = ''
                countArticlesRetrieved = ''
                articleCountScore = ''
                targetAuthorInstitutionalAffiliationArticlePubmedLabel = ''
                pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = ''
                scopusNonTargetAuthorInstitutionalAffiliationSource = ''
                scopusNonTargetAuthorInstitutionalAffiliationScore = ''
                totalArticleScoreWithoutClustering = ''
                clusterScoreAverage = ''
                clusterReliabilityScore = ''
                clusterScoreModificationOfTotalScore = ''
                datePublicationAddedToEntrez = ''
                clusterIdentifier = ''
                doi = ''
                issn = ''
                issue = ''
                journalTitleISOabbreviation = ''
                pages = ''
                timesCited = ''
                volume = ''
                feedbackScoreCites = ''
                feedbackScoreCoAuthorName = ''
                feedbackScoreEmail = ''
                feedbackScoreInstitution = ''
                feedbackScoreJournal = ''
                feedbackScoreJournalSubField = ''
                feedbackScoreKeyword = ''
                feedbackScoreOrcid = ''
                feedbackScoreOrcidCoAuthor = ''
                feedbackScoreOrganization = ''
                feedbackScoreTargetAuthorName = ''
                feedbackScoreYear = ''

                # Common fields
                pmcid = article.get('pmcid', '')
                userAssertion = article.get('userAssertion', '')
                publicationDateStandardized = article.get('publicationDateStandardized', '')
                publicationDateDisplay = article.get('publicationDateDisplay', '')
                publicationTypeCanonical = article.get('publicationType', {}).get('publicationTypeCanonical', '')
                scopusDocID = article.get('scopusDocID', '')
                journalTitleVerbose = article.get('journalTitleVerbose', '').replace('"', '""')
                articleTitle = article.get('articleTitle', '').replace('"', '""')

                # Evidence processing
                evidence = article.get('evidence', {})              

                # New fields (only in new format)
                if is_new_format:
                    authorshipLikelihoodScore = article.get('authorshipLikelihoodScore', '')
                    # Feedback Evidence in new format
                    feedbackEvidence = evidence.get('feedbackEvidence', {})  # Corrected line
                    feedbackScoreCites = feedbackEvidence.get('feedbackScoreCites', '')
                    feedbackScoreCoAuthorName = feedbackEvidence.get('feedbackScoreCoAuthorName', '')
                    feedbackScoreEmail = feedbackEvidence.get('feedbackScoreEmail', '')
                    feedbackScoreInstitution = feedbackEvidence.get('feedbackScoreInstitution', '')
                    feedbackScoreJournal = feedbackEvidence.get('feedbackScoreJournal', '')
                    feedbackScoreJournalSubField = feedbackEvidence.get('feedbackScoreJournalSubField', '')
                    feedbackScoreKeyword = feedbackEvidence.get('feedbackScoreKeyword', '')
                    feedbackScoreOrcid = feedbackEvidence.get('feedbackScoreOrcid', '')
                    feedbackScoreOrcidCoAuthor = feedbackEvidence.get('feedbackScoreOrcidCoAuthor', '')
                    feedbackScoreOrganization = feedbackEvidence.get('feedbackScoreOrganization', '')
                    feedbackScoreTargetAuthorName = feedbackEvidence.get('feedbackScoreTargetAuthorName', '')
                    feedbackScoreYear = feedbackEvidence.get('feedbackScoreYear', '')               

                # Old fields (only in old format)
                if not is_new_format:
                    totalArticleScoreStandardized = article.get('totalArticleScoreStandardized', '')
                    totalArticleScoreNonStandardized = article.get('totalArticleScoreNonStandardized', '')
                    # Accepted/Rejected Evidence
                    acceptedRejectedEvidence = evidence.get('acceptedRejectedEvidence', {})
                    feedbackScoreAccepted = acceptedRejectedEvidence.get('feedbackScoreAccepted', '')
                    feedbackScoreRejected = acceptedRejectedEvidence.get('feedbackScoreRejected', '')
                    feedbackScoreNull = acceptedRejectedEvidence.get('feedbackScoreNull', '')
                    # Clustering Evidence
                    averageClusteringEvidence = evidence.get('averageClusteringEvidence', {})
                    totalArticleScoreWithoutClustering = averageClusteringEvidence.get('totalArticleScoreWithoutClustering', '')
                    clusterScoreAverage = averageClusteringEvidence.get('clusterScoreAverage', '')
                    clusterReliabilityScore = averageClusteringEvidence.get('clusterReliabilityScore', '')
                    clusterScoreModificationOfTotalScore = averageClusteringEvidence.get('clusterScoreModificationOfTotalScore', '')
                    clusterIdentifier = averageClusteringEvidence.get('clusterIdentifier', '')


                # Author Name Evidence
                authorNameEvidence = evidence.get('authorNameEvidence', {})
                if authorNameEvidence:
                    articleAuthorName = authorNameEvidence.get('articleAuthorName', {})
                    articleAuthorName_firstName = articleAuthorName.get('firstName', '')
                    articleAuthorName_lastName = articleAuthorName.get('lastName', '')
                    institutionalAuthorName = authorNameEvidence.get('institutionalAuthorName', {})
                    institutionalAuthorName_firstName = institutionalAuthorName.get('firstName', '')
                    institutionalAuthorName_middleName = institutionalAuthorName.get('middleName', '')
                    institutionalAuthorName_lastName = institutionalAuthorName.get('lastName', '')
                    nameMatchFirstScore = authorNameEvidence.get('nameMatchFirstScore', '')
                    nameMatchFirstType = authorNameEvidence.get('nameMatchFirstType', '')
                    nameMatchMiddleScore = authorNameEvidence.get('nameMatchMiddleScore', '')
                    nameMatchMiddleType = authorNameEvidence.get('nameMatchMiddleType', '')
                    nameMatchLastScore = authorNameEvidence.get('nameMatchLastScore', '')
                    nameMatchLastType = authorNameEvidence.get('nameMatchLastType', '')
                    nameMatchModifierScore = authorNameEvidence.get('nameMatchModifierScore', '')
                    nameScoreTotal = authorNameEvidence.get('nameScoreTotal', '')

                # Email Evidence
                emailEvidence = evidence.get('emailEvidence', {})
                emailMatch = emailEvidence.get('emailMatch', '')
                emailMatchScore = emailEvidence.get('emailMatchScore', '')

                # Journal Category Evidence
                journalCategoryEvidence = evidence.get('journalCategoryEvidence', {})
                journalSubfieldScienceMetrixLabel = journalCategoryEvidence.get('journalSubfieldScienceMetrixLabel', '').replace('"', '""')
                journalSubfieldScienceMetrixID = journalCategoryEvidence.get('journalSubfieldScienceMetrixID', '')
                journalSubfieldDepartment = journalCategoryEvidence.get('journalSubfieldDepartment', '').replace('"', '""')
                journalSubfieldScore = journalCategoryEvidence.get('journalSubfieldScore', '')

                # Relationship Evidence
                relationshipEvidence = evidence.get('relationshipEvidence', {})
                relationshipEvidenceTotalScore = relationshipEvidence.get('relationshipEvidenceTotalScore', '')
                relationshipNegativeMatch = relationshipEvidence.get('relationshipNegativeMatch', {})
                relationshipMinimumTotalScore = relationshipNegativeMatch.get('relationshipMinimumTotalScore', '')
                relationshipNonMatchCount = relationshipNegativeMatch.get('relationshipNonMatchCount', '')
                relationshipNonMatchScore = relationshipNegativeMatch.get('relationshipNonMatchScore', '')

                # Education Year Evidence
                educationYearEvidence = evidence.get('educationYearEvidence', {})
                articleYear = educationYearEvidence.get('articleYear', '')
                identityBachelorYear = educationYearEvidence.get('identityBachelorYear', '')
                discrepancyDegreeYearBachelor = educationYearEvidence.get('discrepancyDegreeYearBachelor', '')
                discrepancyDegreeYearBachelorScore = educationYearEvidence.get('discrepancyDegreeYearBachelorScore', '')
                identityDoctoralYear = educationYearEvidence.get('identityDoctoralYear', '')
                discrepancyDegreeYearDoctoral = educationYearEvidence.get('discrepancyDegreeYearDoctoral', '')
                discrepancyDegreeYearDoctoralScore = educationYearEvidence.get('discrepancyDegreeYearDoctoralScore', '')

                # Gender Evidence
                genderEvidence = evidence.get('genderEvidence', {})
                genderScoreArticle = genderEvidence.get('genderScoreArticle', '')
                genderScoreIdentity = genderEvidence.get('genderScoreIdentity', '')
                genderScoreIdentityArticleDiscrepancy = genderEvidence.get('genderScoreIdentityArticleDiscrepancy', '')

                # Person Type Evidence
                personTypeEvidence = evidence.get('personTypeEvidence', {})
                personType = personTypeEvidence.get('personType', '')
                personTypeScore = personTypeEvidence.get('personTypeScore', '')

                # Article Count Evidence
                articleCountEvidence = evidence.get('articleCountEvidence', {})
                countArticlesRetrieved = articleCountEvidence.get('countArticlesRetrieved', '')
                articleCountScore = articleCountEvidence.get('articleCountScore', '')

                # Affiliation Evidence
                affiliationEvidence = evidence.get('affiliationEvidence', {})
                pubmedAffiliation = affiliationEvidence.get('pubmedTargetAuthorAffiliation', {})
                targetAuthorInstitutionalAffiliationArticlePubmedLabel = pubmedAffiliation.get('targetAuthorInstitutionalAffiliationArticlePubmedLabel', '').replace('"', '""')
                pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = pubmedAffiliation.get('targetAuthorInstitutionalAffiliationMatchTypeScore', '')

                scopusAffiliation = affiliationEvidence.get('scopusNonTargetAuthorAffiliation', {})
                scopusNonTargetAuthorInstitutionalAffiliationSource = scopusAffiliation.get('nonTargetAuthorInstitutionalAffiliationSource', '')
                scopusNonTargetAuthorInstitutionalAffiliationScore = scopusAffiliation.get('nonTargetAuthorInstitutionalAffiliationScore', '')


                # Additional fields
                datePublicationAddedToEntrez = article.get('datePublicationAddedToEntrez', '')
                doi = article.get('doi', '')

                # ISSN processing
                issn_list = article.get('issn', [])
                for issn_info in issn_list:
                    issn_type = issn_info.get('issntype', '')
                    if issn_type in ['Linking', 'Print', 'Electronic']:
                        issn = issn_info.get('issn', '')
                        break

                issue = article.get('issue', '')
                journalTitleISOabbreviation = article.get('journalTitleISOabbreviation', '').replace('"', '""')
                pages = article.get('pages', '')
                timesCited = article.get('timesCited', '')
                volume = article.get('volume', '')

                # Prepare fields for writing
                fields = [
                    '"' + str(personIdentifier) + '"',
                    '"' + str(pmid) + '"',
                    '"' + str(authorshipLikelihoodScore) + '"',
                    '"' + str(pmcid) + '"',
                    '"' + str(totalArticleScoreStandardized) + '"',
                    '"' + str(totalArticleScoreNonStandardized) + '"',
                    '"' + str(userAssertion) + '"',
                    '"' + str(publicationDateDisplay) + '"',
                    '"' + str(publicationDateStandardized) + '"',
                    '"' + str(publicationTypeCanonical) + '"',
                    '"' + str(scopusDocID) + '"',
                    '"' + str(journalTitleVerbose) + '"',
                    '"' + str(articleTitle) + '"',
                    '"' + str(feedbackScoreAccepted) + '"',
                    '"' + str(feedbackScoreRejected) + '"',
                    '"' + str(feedbackScoreNull) + '"',
                    '"' + str(articleAuthorName_firstName) + '"',
                    '"' + str(articleAuthorName_lastName) + '"',
                    '"' + str(institutionalAuthorName_firstName) + '"',
                    '"' + str(institutionalAuthorName_middleName) + '"',
                    '"' + str(institutionalAuthorName_lastName) + '"',
                    '"' + str(nameMatchFirstScore) + '"',
                    '"' + str(nameMatchFirstType) + '"',
                    '"' + str(nameMatchMiddleScore) + '"',
                    '"' + str(nameMatchMiddleType) + '"',
                    '"' + str(nameMatchLastScore) + '"',
                    '"' + str(nameMatchLastType) + '"',
                    '"' + str(nameMatchModifierScore) + '"',
                    '"' + str(nameScoreTotal) + '"',
                    '"' + str(emailMatch) + '"',
                    '"' + str(emailMatchScore) + '"',
                    '"' + str(journalSubfieldScienceMetrixLabel) + '"',
                    '"' + str(journalSubfieldScienceMetrixID) + '"',
                    '"' + str(journalSubfieldDepartment) + '"',
                    '"' + str(journalSubfieldScore) + '"',
                    '"' + str(relationshipEvidenceTotalScore) + '"',
                    '"' + str(relationshipMinimumTotalScore) + '"',
                    '"' + str(relationshipNonMatchCount) + '"',
                    '"' + str(relationshipNonMatchScore) + '"',
                    '"' + str(articleYear) + '"',
                    '"' + str(identityBachelorYear) + '"',
                    '"' + str(discrepancyDegreeYearBachelor) + '"',
                    '"' + str(discrepancyDegreeYearBachelorScore) + '"',
                    '"' + str(identityDoctoralYear) + '"',
                    '"' + str(discrepancyDegreeYearDoctoral) + '"',
                    '"' + str(discrepancyDegreeYearDoctoralScore) + '"',
                    '"' + str(genderScoreArticle) + '"',
                    '"' + str(genderScoreIdentity) + '"',
                    '"' + str(genderScoreIdentityArticleDiscrepancy) + '"',
                    '"' + str(personType) + '"',
                    '"' + str(personTypeScore) + '"',
                    '"' + str(countArticlesRetrieved) + '"',
                    '"' + str(articleCountScore) + '"',
                    '"' + str(targetAuthorInstitutionalAffiliationArticlePubmedLabel) + '"',
                    '"' + str(pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore) + '"',
                    '"' + str(scopusNonTargetAuthorInstitutionalAffiliationSource) + '"',
                    '"' + str(scopusNonTargetAuthorInstitutionalAffiliationScore) + '"',
                    '"' + str(totalArticleScoreWithoutClustering) + '"',
                    '"' + str(clusterScoreAverage) + '"',
                    '"' + str(clusterReliabilityScore) + '"',
                    '"' + str(clusterScoreModificationOfTotalScore) + '"',
                    '"' + str(datePublicationAddedToEntrez) + '"',
                    '"' + str(clusterIdentifier) + '"',
                    '"' + str(doi) + '"',
                    '"' + str(issn) + '"',
                    '"' + str(issue) + '"',
                    '"' + str(journalTitleISOabbreviation) + '"',
                    '"' + str(pages) + '"',
                    '"' + str(timesCited) + '"',
                    '"' + str(volume) + '"',
                    '"' + str(feedbackScoreCites) + '"',
                    '"' + str(feedbackScoreCoAuthorName) + '"',
                    '"' + str(feedbackScoreEmail) + '"',
                    '"' + str(feedbackScoreInstitution) + '"',
                    '"' + str(feedbackScoreJournal) + '"',
                    '"' + str(feedbackScoreJournalSubField) + '"',
                    '"' + str(feedbackScoreKeyword) + '"',
                    '"' + str(feedbackScoreOrcid) + '"',
                    '"' + str(feedbackScoreOrcidCoAuthor) + '"',
                    '"' + str(feedbackScoreOrganization) + '"',
                    '"' + str(feedbackScoreTargetAuthorName) + '"',
                    '"' + str(feedbackScoreYear) + '"',
                ]
                f.write(','.join(fields) + "\n")
            except Exception as e:
                print(f"Error processing article PMID {pmid} for person {personIdentifier}: {e}")
                continue
        count += 1
        print(f"Processed person {personIdentifier}: {article_count} articles")

f.close()












#### The logic of all parts below is similar to the first part, please refer to the first part for explaination ####
#code for person_article_grant_s3 table
f = open(outputPath + 'person_article_grant2.csv','w', encoding='utf-8')

count = 0
for i in range(len(items)):
    try:
        article_temp = len(items[i]['reCiterArticleFeatures'])
    except KeyError:
        print(f"Key 'reCiterArticleFeatures' not found for item {i}: {items[i]}")
        article_temp = 0
    for j in range(article_temp):
        if 'grantEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            grants_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'])
        
            for k in range(grants_temp):
                personIdentifier = items[i]['personIdentifier']
                pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
                articleGrant = items[i]['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['articleGrant']
                grantMatchScore = items[i]['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['grantMatchScore']
                institutionGrant = items[i]['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['institutionGrant']
    
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(articleGrant) + '"' + "," 
                    + str(grantMatchScore)  + "," + '"' + str(institutionGrant) + '"' + "\n")
    count += 1
    print("count person_article_grant:", count)
f.close()



#code for person_article_scopus_non_target_author_affiliation_s3 table
f = open(outputPath + 'person_article_scopus_non_target_author_affiliation2.csv','w', encoding='utf-8')

count = 0
for i in range(len(items)):
    try:
        article_temp = len(items[i]['reCiterArticleFeatures'])
    except KeyError:
        print(f"Key 'reCiterArticleFeatures' not found for item {i}: {items[i]}")
        article_temp = 0
    for j in range(article_temp):
        if 'affiliationEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            if 'scopusNonTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
                if 'nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']:
                    scopusNonTargetAuthorAffiliation_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution'])
            
                    for k in range(scopusNonTargetAuthorAffiliation_temp):
                        personIdentifier = items[i]['personIdentifier']
                        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
                        nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution'][k]
                        #since the nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution field contains more than one featureseparated by comma, and string feature contains comma, we need to disdinguish between this two by the following code
                        count_comma = nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution.count(',')
                        comma_difference = count_comma - 2
                        if comma_difference != 0:
                            nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution = nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution.replace(",", ".", comma_difference)
                        f.write(str(personIdentifier) + "," + str(pmid) + "," + str(nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution) + "\n")
    count += 1
    print("count scopus_non_target_author_affiliation:", count)
f.close()


#code for person_article_scopus_target_author_affiliation_s3 table
f = open(outputPath + 'person_article_scopus_target_author_affiliation2.csv','w', encoding='utf-8')

count = 0
for i in range(len(items)):
    try:
        article_temp = len(items[i]['reCiterArticleFeatures'])
    except KeyError:
        print(f"Key 'reCiterArticleFeatures' not found for item {i}: {items[i]}")
        article_temp = 0
    for j in range(article_temp):
        if 'affiliationEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            if 'scopusTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
                scopusTargetAuthorAffiliation_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'])
            
                for k in range(scopusTargetAuthorAffiliation_temp):
                    personIdentifier = items[i]['personIdentifier']
                    pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
                    targetAuthorInstitutionalAffiliationSource = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationSource']
                    if 'scopusTargetAuthorInstitutionalAffiliationIdentity' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]:
                        scopusTargetAuthorInstitutionalAffiliationIdentity = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationIdentity']
                    else:
                        scopusTargetAuthorInstitutionalAffiliationIdentity = ""
                    if 'targetAuthorInstitutionalAffiliationArticleScopusLabel' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]:
                        targetAuthorInstitutionalAffiliationArticleScopusLabel = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationArticleScopusLabel']
                    else:
                        targetAuthorInstitutionalAffiliationArticleScopusLabel = ""
                    targetAuthorInstitutionalAffiliationArticleScopusAffiliationId = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationArticleScopusAffiliationId']
                    targetAuthorInstitutionalAffiliationMatchType = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationMatchType']
                    targetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationMatchTypeScore']

                    f.write(str(personIdentifier) + "," + str(pmid) + "," + str(targetAuthorInstitutionalAffiliationSource) + "," 
                        + '"' + str(scopusTargetAuthorInstitutionalAffiliationIdentity) + '"' + "," + '"' + str(targetAuthorInstitutionalAffiliationArticleScopusLabel) + '"' + "," + str(targetAuthorInstitutionalAffiliationArticleScopusAffiliationId) + "," 
                        + str(targetAuthorInstitutionalAffiliationMatchType) + "," + str(targetAuthorInstitutionalAffiliationMatchTypeScore) + "\n")
    count += 1
    print("count scopus_target_author_affiliation:", count)
f.close()



#code for person_article_department_s3 table 
f = open(outputPath + 'person_article_department2.csv','w', encoding='utf-8')

count = 0
for i in range(len(items)):
    try:
        article_temp = len(items[i]['reCiterArticleFeatures'])
    except KeyError:
        print(f"Key 'reCiterArticleFeatures' not found for item {i}: {items[i]}")
        article_temp = 0
    for j in range(article_temp):
        if 'organizationalUnitEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            organizationalUnit_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'])
        
            for k in range(organizationalUnit_temp):
                personIdentifier = items[i]['personIdentifier']
                pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
                identityOrganizationalUnit = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['identityOrganizationalUnit']
                identityOrganizationalUnit = identityOrganizationalUnit.replace('"', '""')
                articleAffiliation = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['articleAffiliation']
                articleAffiliation = articleAffiliation.replace('"', '""')
                organizationalUnitType = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitType']
                organizationalUnitMatchingScore = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitMatchingScore']
                if 'organizationalUnitModifier' in items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]:
                    organizationalUnitModifier = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitModifier']
                else:
                    organizationalUnitModifier = ""
                organizationalUnitModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitModifierScore']
                
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(identityOrganizationalUnit) + '"' + "," 
                    + '"' + str(articleAffiliation) + '"' + "," + str(organizationalUnitType) + "," 
                    + str(organizationalUnitMatchingScore) + "," + str(organizationalUnitModifier) + "," + str(organizationalUnitModifierScore) + "\n")
    count += 1
    print("count person_article_department:", count)
f.close()



#code for person_article_relationship_s3 table
f = open(outputPath + 'person_article_relationship2.csv','w', encoding='utf-8')

#capture misspelling key in the content
misspelling_list = []
count = 0
for i in range(len(items)):
    try:
        article_temp = len(items[i]['reCiterArticleFeatures'])
    except KeyError:
        print(f"Key 'reCiterArticleFeatures' not found for item {i}: {items[i]}")
        article_temp = 0
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
        if 'relationshipEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
            #the nested key structure is different for every file, so we need to consider two conditions here
            if 'relationshipEvidenceTotalScore' not in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                relationshipPositiveMatch_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'])
                for k in range(relationshipPositiveMatch_temp):
                    relationshipNameArticle_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameArticle']['firstName']
                    relationshipNameArticle_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameArticle']['lastName']
                    
                    if 'relationshipNameIdenity' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]:
                        misspelling_list.append((personIdentifier, pmid))
                        relationshipNameIdentity_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameIdenity']['firstName']
                        relationshipNameIdentity_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameIdenity']['lastName']
                    else:
                        relationshipNameIdentity_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameIdentity']['firstName']
                        relationshipNameIdentity_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipNameIdentity']['lastName']
                    
                    if 'relationshipType' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]:
                        relationshipType = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipType']
                    else:
                        relationshipType = ""
                    relationshipMatchType = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchType'] 
                    relationshipMatchingScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchingScore']
                    relationshipVerboseMatchModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipVerboseMatchModifierScore']
                    relationshipMatchModifierMentor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchModifierMentor']
                    relationshipMatchModifierMentorSeniorAuthor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchModifierMentorSeniorAuthor']
                    relationshipMatchModifierManager = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchModifierManager']
                    relationshipMatchModifierManagerSeniorAuthor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence'][k]['relationshipMatchModifierManagerSeniorAuthor']

                    f.write(str(personIdentifier) + "," + str(pmid) + "," + str(relationshipNameArticle_firstName) + "," 
                        + str(relationshipNameArticle_lastName) + "," + str(relationshipNameIdentity_firstName) + "," 
                        + str(relationshipNameIdentity_lastName) + "," + '"' + str(relationshipType) + '"' + "," + str(relationshipMatchType) + ","
                        + str(relationshipMatchingScore) + "," + str(relationshipVerboseMatchModifierScore) + "," + str(relationshipMatchModifierMentor) + ","
                        + str(relationshipMatchModifierMentorSeniorAuthor) + "," + str(relationshipMatchModifierManager) + "," + str(relationshipMatchModifierManagerSeniorAuthor) + "\n")                    
            
            if 'relationshipPositiveMatch' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                relationshipPositiveMatch_temp = len(items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'])
                for k in range(relationshipPositiveMatch_temp):
                    relationshipNameArticle_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameArticle']['firstName']
                    relationshipNameArticle_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameArticle']['lastName']
                    
                    if 'relationshipNameIdenity' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]:
                        misspelling_list.append((personIdentifier, pmid))
                        relationshipNameIdentity_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdenity']['firstName']
                        relationshipNameIdentity_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdenity']['lastName']
                    else:
                        relationshipNameIdentity_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdentity']['firstName']
                        relationshipNameIdentity_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdentity']['lastName']
                    if 'relationshipType' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]:
                        relationshipType = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipType']
                    else:
                        relationshipType = ""
                    relationshipMatchType = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchType'] 
                    relationshipMatchingScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchingScore']
                    relationshipVerboseMatchModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipVerboseMatchModifierScore']
                    relationshipMatchModifierMentor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierMentor']
                    relationshipMatchModifierMentorSeniorAuthor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierMentorSeniorAuthor']
                    relationshipMatchModifierManager = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierManager']
                    relationshipMatchModifierManagerSeniorAuthor = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierManagerSeniorAuthor']
                    
                    f.write(str(personIdentifier) + "," + str(pmid) + "," + str(relationshipNameArticle_firstName) + "," 
                        + str(relationshipNameArticle_lastName) + "," + str(relationshipNameIdentity_firstName) + "," 
                        + str(relationshipNameIdentity_lastName) + "," + '"' + str(relationshipType) + '"' + "," + str(relationshipMatchType) + ","
                        + str(relationshipMatchingScore) + "," + str(relationshipVerboseMatchModifierScore) + "," + str(relationshipMatchModifierMentor) + ","
                        + str(relationshipMatchModifierMentorSeniorAuthor) + "," + str(relationshipMatchModifierManager) + "," + str(relationshipMatchModifierManagerSeniorAuthor) + "\n")
    count += 1
    print("count person_article_relationship:", count)
f.close()
print(misspelling_list)

#code for person_article_author_s3 table
f = open(outputPath + 'person_article_author2.csv','w', encoding='utf-8')

#some article is group authorship, so there is no record for authors in the file, here we use a list to record this 
no_reCiterArticleAuthorFeatures_list =[]
count = 0

for i in range(len(items)):
  try:
    article_temp = len(items[i]['reCiterArticleFeatures']) 
  except KeyError as e:
    print(f"Error getting article features: {e}")
    continue
  
  for j in range(article_temp):
    try:
      personIdentifier = items[i]['personIdentifier']
      pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
      
      if 'reCiterArticleAuthorFeatures' in items[i]['reCiterArticleFeatures'][j]:
        # process authors
        author_temp = len(items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'])
        
        for k in range(author_temp):
          if 'firstName' in items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
            firstName = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['firstName']  
          else:
            firstName = ""
            
          lastName = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['lastName']                
          targetAuthor = int(items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['targetAuthor'])                
          rank = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['rank']
          
          if 'orcid' in items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
            orcid = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['orcid']
          else:
            orcid = ""
            
          if 'equalContrib' in items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
            equalContrib = items[i]['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['equalContrib']
          else:
            equalContrib = ""
            
          f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(firstName) + '"' + "," + '"' + str(lastName) + '"' + "," + str(targetAuthor) + "," + str(rank) + "," + str(orcid) + "," + str(equalContrib) + "\n")
      
      else:
        no_reCiterArticleAuthorFeatures_list.append((personIdentifier, pmid))
        
      count += 1  
      print(f"Processed person {count}")
    
    except Exception as e:    
      print(f"Error processing person {i}, PMID {pmid}: {e}")
      continue
      
print("Finished processing authors")  
f.close()


#code for person table
f = open(outputPath + 'person2.csv','w', encoding='utf-8')

count = 0
for i in range(len(items)):
    personIdentifier = items[i]['personIdentifier']
    dateAdded = items[i]['dateAdded']
    dateUpdated = items[i]['dateUpdated']
    precision = items[i]['precision']
    recall = items[i]['recall']
    countSuggestedArticles = items[i]['countSuggestedArticles']
    if 'countPendingArticles' in items[i]:
        countPendingArticles = items[i]['countPendingArticles']
    else:
        countPendingArticles = 0
    overallAccuracy = items[i]['overallAccuracy']
    mode = items[i]['mode']

    f.write(str(personIdentifier) + "," + str(dateAdded) + "," + str(dateUpdated) + "," 
                + str(precision) + "," + str(recall) + "," 
                + str(countSuggestedArticles) + "," + str(countPendingArticles) + "," +  str(overallAccuracy) + "," + str(mode) + "\n")
    count += 1
    print("count person:", count)
f.close()

#code for person_article_keyword_s3 table
#open a csv file
f = open(outputPath + 'person_article_keyword2.csv','w', encoding='utf-8')

#use count to record the number of person we have finished feature extraction
count = 0
#extract all required nested features 
for i in range(len(items)):
    try:
        article_temp = len(items[i]['reCiterArticleFeatures'])
    except KeyError:
        print(f"Key 'reCiterArticleFeatures' not found for item {i}: {items[i]}")
        article_temp = 0
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
        if 'articleKeywords' in items[i]['reCiterArticleFeatures'][j]:
            keywords_temp = len(items[i]['reCiterArticleFeatures'][j]['articleKeywords'])
            for k in range(keywords_temp):
                if 'keyword' in items[i]['reCiterArticleFeatures'][j]['articleKeywords'][k]:
                    keyword = items[i]['reCiterArticleFeatures'][j]['articleKeywords'][k]['keyword']
                else: 
                    keyword = ""
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(keyword) + '"' + "\n")
    count += 1
    print("count person_article_keyword:", count)            
f.close()
