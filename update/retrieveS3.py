import json
from boto3.dynamodb.conditions import Key, Attr
import time
import os
import csv
from init import pymysql
import MySQLdb

import boto3
import os

dynamodb = boto3.resource('dynamodb')

def download_directory_from_s3(bucket_name, remote_directory_name, local_directory_name):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
    number = 0
    for object in bucket.objects.filter(Prefix=remote_directory_name):
        number = number + 1
        print(object)        
        local_path = f"{local_directory_name}/"
        file_name = local_path + "/" + object.key.removeprefix(remote_directory_name)
        bucket.download_file(object.key, file_name)



def downloadDirectoryFroms3(bucketName,remoteDirectoryName):
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucketName)
    number = 0
    for object in bucket.objects.filter(Prefix = ''):
        number = number + 1
#        if number == 100:
#            break
        print(object)
        txt = object.key 
        object.key = txt.replace(remoteDirectoryName, "")
        if not os.path.exists('tempS3Output/' + object.key):
            os.makedirs('tempS3Output/' + object.key)
        bucket.download_file(object.key,'tempS3Output/' + object.key)


def scan_table(table_name): #runtime: about 15min
    #record time for scan the entire table
    print(dynamodb)
    start = time.time()
    table = dynamodb.Table(table_name)

    response = table.scan()

    items = response['Items']

    #continue to gat all records in the table, using ExclusiveStartKey
    while True:
        print(len(response['Items']))
        if response.get('LastEvaluatedKey'):
            response = table.scan(
                ExclusiveStartKey = response['LastEvaluatedKey']
                )
            items += response['Items']
        else:           
            break
    print('execution time:', time.time() - start)
    
    return items


# This is where the raw JSON files downloaded from s3 go. Files have the format "[personIdentifier]".

originalDataPath = 'temp/s3Output/' 

# This is where the parsed CSV files go.
outputPath = 'temp/parsedOutput/'


# Call scan_table function for analysis
identities = scan_table('Identity')

# Output verbose form of Identity table

# print(identities)

print("Count items from DynamoDB Identity table:", len(identities)) 


# For testing purposes, comment this line out if you have the files and wish to re-run the script without downloading all the files

download_directory_from_s3('reciter-dynamodb', 'AnalysisOutput','temp/s3Output')


person_list = []
for filename in os.listdir(originalDataPath):
    person_list.append(filename)

## If you see this error "UnicodeDecodeError: 'utf-8' codec can't decode byte 0x80 in position 3131: invalid start byte",
## it's because you haven't removed the ".DS_Store" file

try:
    os.remove('.DS_Store')
except OSError:
    pass

try:
    os.remove('.gitkeep')
except OSError:
    pass

person_list.sort()
print(len(person_list))


#use the directory to read files in 
items = []
for item in person_list:
    #record time
    start = time.time()
    for line in open(originalDataPath + '{}'.format(item), 'r', encoding='utf-8'): 
        items.append(json.loads(line))
    print('execution time:', time.time() - start)
print(len(items))



# prepare query and data
f = open(outputPath + 'identity.csv','w', encoding='utf-8')

count = 0

for i in range(len(identities)):
    if 'uid' in identities[i]:
        personIdentifier = identities[i]['uid']
    if 'title' in identities[i]['identity']:
        title = identities[i]['identity']['title']
    else:
        title = ''
    if 'firstName' in identities[i]['identity']['primaryName']:
        firstName = identities[i]['identity']['primaryName']['firstName']
    else:
        firstName = ''
    if 'middleName' in identities[i]['identity']['primaryName']:
        middleName = identities[i]['identity']['primaryName']['middleName']
    else:
        middleName = ''
    if 'lastName' in identities[i]['identity']['primaryName']:
        lastName = identities[i]['identity']['primaryName']['lastName']
    else:
        lastName = ''
    if 'primaryEmail' in identities[i]['identity']:
        primaryEmail = identities[i]['identity']['primaryEmail']
    else:
        primaryEmail = ''        
    if 'primaryOrganizationalUnit' in identities[i]['identity']:
        primaryOrganizationalUnit = identities[i]['identity']['primaryOrganizationalUnit']
    else:
        primaryOrganizationalUnit = ''
    if 'primaryInstitution' in identities[i]['identity']:
        primaryInstitution = identities[i]['identity']['primaryInstitution']
    else:
        primaryInstitution = ''

    f.write("\"" + str(personIdentifier) + "\"" + "\t" + "\"" + 
            str(title) + "\"" + "\t" + "\"" + 
            str(firstName) + "\"" + "\t" + "\"" + 
            str(middleName) + "\"" + "\t" + "\"" + 
            str(lastName) + "\"" + "\t" + "\"" + 
            str(primaryEmail) + "\"" + "\t" + "\"" + 
            str(primaryOrganizationalUnit) + "\"" + "\t" + "\"" + 
            str(primaryInstitution) + "\"" + 
            "\n")
    count += 1
    print("Identities imported into temp table:", count)

f.close()


f = open(outputPath + 'person_person_type.csv','w', encoding='utf-8')

for i in range(len(identities)):
    a = identities[i]['identity']
    if 'uid' in a.keys():
        personIdentifier = a['uid']
    else:
        print("uid key not found")
    if 'personTypes' in a.keys():
        personType = a['personTypes']
        for each_person_type in personType:
            f.write(str(personIdentifier) + "," + str(each_person_type) + "\n")
    else:
        print("Person type not found for", personIdentifier)
f.close()




#code for person_article_s3 table
#open a csv file
f = open(outputPath + 'person_article2.csv','w', encoding='utf-8')

#use count to record the number of person we have finished feature extraction
count = 0
#extract all required nested features 
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']

        totalArticleScoreStandardized = items[i]['reCiterArticleFeatures'][j]['totalArticleScoreStandardized']
        totalArticleScoreNonStandardized = items[i]['reCiterArticleFeatures'][j]['totalArticleScoreNonStandardized']
        userAssertion = items[i]['reCiterArticleFeatures'][j]['userAssertion']
        publicationDateStandardized = items[i]['reCiterArticleFeatures'][j]['publicationDateStandardized']
        if 'publicationTypeCanonical' in items[i]['reCiterArticleFeatures'][j]['publicationType']:
            publicationTypeCanonical = items[i]['reCiterArticleFeatures'][j]['publicationType']['publicationTypeCanonical']
        else:
            publicationTypeCanonical = ""
        # example1: when you get key error, check whether the key exists in dynamodb or not
        if 'scopusDocID' in items[i]['reCiterArticleFeatures'][j]:
            scopusDocID = items[i]['reCiterArticleFeatures'][j]['scopusDocID']
        else:
            scopusDocID = ""

        if 'pmcid' in items[i]['reCiterArticleFeatures'][j]:
            pmcid = items[i]['reCiterArticleFeatures'][j]['pmcid']
        else:
            pmcid = ""

        journalTitleVerbose = items[i]['reCiterArticleFeatures'][j]['journalTitleVerbose']
        journalTitleVerbose = journalTitleVerbose.replace('"', '""')
        if 'articleTitle' in items[i]['reCiterArticleFeatures'][j]:
            articleTitle = items[i]['reCiterArticleFeatures'][j]['articleTitle']
            articleTitle = articleTitle.replace('"', '""')
        else:
            articleTitle = ""

        if 'reCiterArticleAuthorFeatures' not in items[i]['reCiterArticleFeatures'][j]:
            largeGroupAuthorship = True
        else:
            largeGroupAuthorship = False
        if 'evidence' in items[i]['reCiterArticleFeatures'][j]:
            if 'acceptedRejectedEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'feedbackScoreAccepted' in items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                    feedbackScoreAccepted = items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreAccepted']
                else: 
                    feedbackScoreAccepted = 0
                if 'feedbackScoreRejected' in items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                    feedbackScoreRejected = items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreRejected']
                else: 
                    feedbackScoreRejected = 0
                if 'feedbackScoreNull' in items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                    feedbackScoreNull = items[i]['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreNull']
                else: 
                    feedbackScoreNull = 0
            if 'authorNameEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'articleAuthorName' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                    if 'firstName' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']: 
                        articleAuthorName_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']['firstName']
                    else:
                        articleAuthorName_firstName = ""
                    articleAuthorName_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']['lastName']
                else:
                    articleAuthorName_firstName, articleAuthorName_lastName = "", ""
                institutionalAuthorName_firstName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['firstName']
                if 'middleName' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']:
                    institutionalAuthorName_middleName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['middleName']
                else:
                    institutionalAuthorName_middleName = ""
                institutionalAuthorName_lastName = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['lastName']
                nameMatchFirstScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstScore']
                if 'nameMatchFirstType' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                    nameMatchFirstType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstType']
                nameMatchMiddleScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleScore']
                if 'nameMatchMiddleType' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                    nameMatchMiddleType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleType']
                nameMatchLastScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastScore']
                if 'nameMatchLastType' in items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                    nameMatchLastType = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastType']
                nameMatchModifierScore = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchModifierScore']
                nameScoreTotal = items[i]['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameScoreTotal']

            if 'emailEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                emailMatch = items[i]['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatch']
                if 'false' in emailMatch:
                    emailMatch = ""
                emailMatchScore = items[i]['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatchScore']
            else:
                emailMatch, emailMatchScore = "", 0
            
            if 'journalCategoryEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                journalSubfieldScienceMetrixLabel = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixLabel']
                journalSubfieldScienceMetrixLabel = journalSubfieldScienceMetrixLabel.replace('"', '""')
                journalSubfieldScienceMetrixID = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixID']
                journalSubfieldDepartment = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldDepartment']
                journalSubfieldDepartment = journalSubfieldDepartment.replace('"', '""')
                journalSubfieldScore = items[i]['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScore']
            else:
                journalSubfieldScienceMetrixLabel, journalSubfieldScienceMetrixID, journalSubfieldDepartment, journalSubfieldScore = "", "", "", 0
            
            if 'relationshipEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'relationshipEvidenceTotalScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                    relationshipEvidenceTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipEvidenceTotalScore']
                else:
                    relationshipEvidenceTotalScore = 0
                if 'relationshipNegativeMatch' in items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                    relationshipMinimumTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipMinimumTotalScore']
                    relationshipNonMatchCount = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchCount']
                    relationshipNonMatchScore = items[i]['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchScore']
                else:
                    relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = 0, 0, 0
            else:
                relationshipEvidenceTotalScore, relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = 0, 0, 0, 0
            
            if 'educationYearEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'articleYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    articleYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['articleYear']
                else:
                    articleYear = 0
                if 'identityBachelorYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    identityBachelorYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityBachelorYear']
                else:
                    identityBachelorYear = ""
                if 'discrepancyDegreeYearBachelor' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    discrepancyDegreeYearBachelor = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelor']
                else:
                    discrepancyDegreeYearBachelor = 0
                if 'discrepancyDegreeYearBachelorScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    discrepancyDegreeYearBachelorScore = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelorScore']
                else:
                    discrepancyDegreeYearBachelorScore = 0
                if 'identityDoctoralYear' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    identityDoctoralYear = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityDoctoralYear']
                else:
                    identityDoctoralYear = ""
                if 'discrepancyDegreeYearDoctoral' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    discrepancyDegreeYearDoctoral = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoral']
                else:
                    discrepancyDegreeYearDoctoral = 0
                if 'discrepancyDegreeYearDoctoralScore' in items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                    discrepancyDegreeYearDoctoralScore = items[i]['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoralScore']
                else:
                    discrepancyDegreeYearDoctoralScore = 0
            else:
                articleYear, identityBachelorYear, discrepancyDegreeYearBachelor, discrepancyDegreeYearBachelorScore, identityDoctoralYear, discrepancyDegreeYearDoctoral, discrepancyDegreeYearDoctoralScore = 0, "", 0, 0, "", 0, 0
            
            if 'genderEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                genderScoreArticle = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreArticle']
                genderScoreIdentity = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentity']
                genderScoreIdentityArticleDiscrepancy = items[i]['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentityArticleDiscrepancy']
            else:
                genderScoreArticle, genderScoreIdentity, genderScoreIdentityArticleDiscrepancy = 0, 0, 0
            
            if 'personTypeEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                personType = items[i]['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personType']
                personTypeScore = items[i]['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personTypeScore']
            else:
                personType, personTypeScore = "", 0
            
            if 'articleCountEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                countArticlesRetrieved = items[i]['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['countArticlesRetrieved']
                articleCountScore = items[i]['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['articleCountScore']
            else:
                countArticlesRetrieved,  articleCountScore= 0,0
            
            if 'affiliationEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'pubmedTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
                    targetAuthorInstitutionalAffiliationArticlePubmedLabel = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationArticlePubmedLabel']
                    targetAuthorInstitutionalAffiliationArticlePubmedLabel = targetAuthorInstitutionalAffiliationArticlePubmedLabel.replace('"', '""')
                    pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationMatchTypeScore']
                else:
                    targetAuthorInstitutionalAffiliationArticlePubmedLabel, pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = "", 0
            
            if 'affiliationEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                if 'scopusNonTargetAuthorAffiliation' in items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
                    scopusNonTargetAuthorInstitutionalAffiliationSource = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationSource']
                    scopusNonTargetAuthorInstitutionalAffiliationScore = items[i]['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationScore']
                else:
                    scopusNonTargetAuthorInstitutionalAffiliationSource, scopusNonTargetAuthorInstitutionalAffiliationScore= "", 0

            if 'averageClusteringEvidence' in items[i]['reCiterArticleFeatures'][j]['evidence']:
                totalArticleScoreWithoutClustering = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['totalArticleScoreWithoutClustering']
                clusterScoreAverage = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreAverage']
                clusterReliabilityScore = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterReliabilityScore']
                clusterScoreModificationOfTotalScore = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreModificationOfTotalScore']
                if 'clusterIdentifier' in items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']:
                    clusterIdentifier = items[i]['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterIdentifier']
                else :
                    clusterIdentifier = 0

        if 'publicationDateDisplay' in items[i]['reCiterArticleFeatures'][j]:
            publicationDateDisplay = items[i]['reCiterArticleFeatures'][j]['publicationDateDisplay']
        else:
            publicationDateDisplay = ""

        if 'datePublicationAddedToEntrez' in items[i]['reCiterArticleFeatures'][j]:
            datePublicationAddedToEntrez = items[i]['reCiterArticleFeatures'][j]['datePublicationAddedToEntrez']
        else:
            datePublicationAddedToEntrez = ""

        if 'doi' in items[i]['reCiterArticleFeatures'][j]:
            doi = items[i]['reCiterArticleFeatures'][j]['doi']
        else: 
            doi = ""
        #print(items[i]['reCiterArticleFeatures'][j])
        if 'issn' in items[i]['reCiterArticleFeatures'][j]:
            issn_temp = len(items[i]['reCiterArticleFeatures'][j]['issn'])
            for k in range(issn_temp):
                issntype = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issntype']
                if issntype == 'Linking':
                    issn = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
                if issntype == 'Print':
                    issn = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
                if issntype == 'Electronic':
                    issn = items[i]['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
        else:
            issn = ""

        if 'issue' in items[i]['reCiterArticleFeatures'][j]:
            issue = items[i]['reCiterArticleFeatures'][j]['issue']
        else: 
            issue = ""
        if 'journalTitleISOabbreviation' in items[i]['reCiterArticleFeatures'][j]:
            journalTitleISOabbreviation = items[i]['reCiterArticleFeatures'][j]['journalTitleISOabbreviation']
            journalTitleISOabbreviation = journalTitleISOabbreviation.replace('"', '""')
        else:
            journalTitleISOabbreviation = ""
        if 'pages' in items[i]['reCiterArticleFeatures'][j]:
            pages = items[i]['reCiterArticleFeatures'][j]['pages']
        else:
            pages = ""
        if 'timesCited' in items[i]['reCiterArticleFeatures'][j]:
            timesCited = items[i]['reCiterArticleFeatures'][j]['timesCited']
        else: 
            timesCited = 0
        if 'volume' in items[i]['reCiterArticleFeatures'][j]:
            volume = items[i]['reCiterArticleFeatures'][j]['volume']
        else:
            volume = ""
        
        #write all extracted features into csv file
        #some string value may contain a comma, in this case, we need to double quote the output value, for example, '"' + str(journalSubfieldScienceMetrixLabel) + '"'
        f.write('"' + str(personIdentifier) + '"' + "," + '"' + str(pmid) + '"' + "," + '"' + str(pmcid) + '"' + "," + '"' + str(totalArticleScoreStandardized) + '"' + "," 
                + '"' + str(totalArticleScoreNonStandardized) + '"' + "," + '"' + str(userAssertion) + '"' + "," 
                + '"' + str(publicationDateDisplay) + '"' + "," + '"' + str(publicationDateStandardized) + '"' + "," + '"' + str(publicationTypeCanonical) + '"' + ","
                + '"' + str(scopusDocID) + '"' + ","  + '"' + str(journalTitleVerbose) + '"' + "," + '"' + str(articleTitle) + '"' + "," + '"' + str(feedbackScoreAccepted) + '"' + "," + '"' + str(feedbackScoreRejected) + '"' + "," + '"' + str(feedbackScoreNull) + '"' + "," 
                + '"' + str(articleAuthorName_firstName) + '"' + "," + '"' + str(articleAuthorName_lastName) + '"' + "," + '"' + str(institutionalAuthorName_firstName) + '"' + "," + '"' + str(institutionalAuthorName_middleName) + '"' + "," + '"' + str(institutionalAuthorName_lastName) + '"' + ","
                + '"' + str(nameMatchFirstScore) + '"' + "," + '"' + str(nameMatchFirstType) + '"' + "," + '"' + str(nameMatchMiddleScore) + '"' + "," + '"' + str(nameMatchMiddleType) + '"' + ","
                + '"' + str(nameMatchLastScore) + '"' + "," + '"' + str(nameMatchLastType) + '"' + "," + '"' + str(nameMatchModifierScore) + '"' + "," + '"' + str(nameScoreTotal) + '"' + ","
                + '"' + str(emailMatch) + '"' + "," + '"' + str(emailMatchScore) + '"' + "," 
                + '"' + str(journalSubfieldScienceMetrixLabel) + '"' + "," + '"' + str(journalSubfieldScienceMetrixID) + '"' + "," + '"' + str(journalSubfieldDepartment) + '"' + "," + '"' + str(journalSubfieldScore) + '"' + "," 
                + '"' + str(relationshipEvidenceTotalScore) + '"' + "," + '"' + str(relationshipMinimumTotalScore) + '"' + "," + '"' + str(relationshipNonMatchCount) + '"' + "," + '"' + str(relationshipNonMatchScore) + '"' + ","
                + '"' + str(articleYear) + '"' + "," + '"' + str(identityBachelorYear) + '"' + "," + '"' + str(discrepancyDegreeYearBachelor) + '"' + "," + '"' + str(discrepancyDegreeYearBachelorScore) + '"' + ","
                + '"' + str(identityDoctoralYear) + '"' + "," + '"' + str(discrepancyDegreeYearDoctoral) + '"' + "," + '"' + str(discrepancyDegreeYearDoctoralScore) + '"' + "," 
                + '"' + str(genderScoreArticle) + '"' + "," + '"' + str(genderScoreIdentity) + '"' + "," + '"' + str(genderScoreIdentityArticleDiscrepancy) + '"' + "," 
                + '"' + str(personType) + '"' + "," + '"' + str(personTypeScore) + '"' + ","
                + '"' + str(countArticlesRetrieved) + '"' + "," + '"' + str(articleCountScore) + '"' + ","
                + '"' + str(targetAuthorInstitutionalAffiliationArticlePubmedLabel) + '"' + "," + '"' + str(pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore) + '"' + "," + '"' + str(scopusNonTargetAuthorInstitutionalAffiliationSource) + '"' + "," + '"' + str(scopusNonTargetAuthorInstitutionalAffiliationScore) + '"' + ","
                + '"' + str(totalArticleScoreWithoutClustering) + '"' + "," + '"' + str(clusterScoreAverage) + '"' + "," + '"' + str(clusterReliabilityScore) + '"' + "," + '"' + str(clusterScoreModificationOfTotalScore) + '"' + ","
                + '"' + str(datePublicationAddedToEntrez) + '"' + "," + '"' + str(clusterIdentifier) + '"' + "," + '"' + str(doi) + '"' + "," + '"' + str(issn) + '"' + "," + '"' + str(issue) + '"' + "," + '"' + str(journalTitleISOabbreviation) + '"'  + "," + '"' + str(pages) + '"' + "," + '"' + str(timesCited) + '"' + "," + '"' + str(volume) + '"'
                + "\n")
    count += 1
    print("count person_article:", count)
f.close()


#### The logic of all parts below is similar to the first part, please refer to the first part for explaination ####
#code for person_article_grant_s3 table
f = open(outputPath + 'person_article_grant2.csv','w', encoding='utf-8')

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterArticleFeatures'])
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
    article_temp = len(items[i]['reCiterArticleFeatures'])
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
    article_temp = len(items[i]['reCiterArticleFeatures'])
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
    article_temp = len(items[i]['reCiterArticleFeatures'])
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
    article_temp = len(items[i]['reCiterArticleFeatures'])
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
    article_temp = len(items[i]['reCiterArticleFeatures'])
    for j in range(article_temp):
        personIdentifier = items[i]['personIdentifier']
        pmid = items[i]['reCiterArticleFeatures'][j]['pmid']
        if 'reCiterArticleAuthorFeatures' in items[i]['reCiterArticleFeatures'][j]:
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
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(firstName) + '"' + "," + '"' + str(lastName) + '"' + "," + str(targetAuthor) + "," + str(rank) + "," + str(orcid) + "\n")
        else:
            no_reCiterArticleAuthorFeatures_list.append((personIdentifier, pmid))
    count += 1
    print("count person_article_author:", count)
f.close()
print(no_reCiterArticleAuthorFeatures_list) 



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
    article_temp = len(items[i]['reCiterArticleFeatures'])
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
