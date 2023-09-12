import boto3
from boto3.dynamodb.conditions import Key, Attr
import time
import json
import csv
import decimal
from init import pymysql
import MySQLdb
import os

dynamodb = boto3.resource('dynamodb')

def scan_table(table_name, is_filter_expression): #runtime: about 15min
    #record time for scan the entire table
    print(dynamodb)
    start = time.time()
    table = dynamodb.Table(table_name)

    response = table.scan(
        FilterExpression = Attr('usingS3').eq(0),
        )

    items = response['Items']

    #continue to gat all records in the table, using ExclusiveStartKey
    while True:
        print(len(response['Items']))
        if response.get('LastEvaluatedKey'):
            if is_filter_expression: 
                response = table.scan(
                    ExclusiveStartKey = response['LastEvaluatedKey'],
                    FilterExpression = Attr('usingS3').eq(0)
                    )
                items += response['Items']
            else:
                response = table.scan(
                    ExclusiveStartKey = response['LastEvaluatedKey']
                    )
                items += response['Items']
        else:           
            break
    print('execution time:', time.time() - start)
    
    return items

outputPath = 'temp/parsedOutput/'

# outputPath = '/Users/paulalbert/Dropbox/Index/ReCiter/AdHocDatabaseImport/revised/parsedOutput/'

#call scan_table function for analysis
items = scan_table('Analysis', True)
print("Count Items from DynamoDB Analysis table:", len(items)) 

#use a list to store (personIdentifier, number of articles for this person)
count_articles = []
#also check if there is anyone in the table with 0 article
no_article_person_list = []
for i in items:
    count_articles.append((i['reCiterFeature']['personIdentifier'], len(i['reCiterFeature']['reCiterArticleFeatures'])))
    if len(i['reCiterFeature']['reCiterArticleFeatures']) == 0:
        no_article_person_list.append(i['reCiterFeature']['personIdentifier'])
print("Count Items from count_articles list:", len(count_articles))
print(len(no_article_person_list))



#code for person_article table
#open a csv file in the directory you preferred
f = open(outputPath + 'person_article1.csv','w', encoding='utf-8')
#write column names into file
f.write("personIdentifier," + "pmid," + "pmcid," + "totalArticleScoreStandardized," + "totalArticleScoreNonStandardized," 
        + "userAssertion," + "publicationDateDisplay," + "publicationDateStandardized," + "publicationTypeCanonical,"
        + "scopusDocID," + "journalTitleVerbose," + "articleTitle," + "feedbackScoreAccepted," + "feedbackScoreRejected," + "feedbackScoreNull," 
        + "articleAuthorNameFirstName," + "articleAuthorNameLastName," + "institutionalAuthorNameFirstName," + "institutionalAuthorNameMiddleName," + "institutionalAuthorNameLastName,"
        + "nameMatchFirstScore," + "nameMatchFirstType," + "nameMatchMiddleScore," + "nameMatchMiddleType," + "nameMatchLastScore," + "nameMatchLastType," + "nameMatchModifierScore," + "nameScoreTotal,"
        + "emailMatch," + "emailMatchScore," + "journalSubfieldScienceMetrixLabel," + "journalSubfieldScienceMetrixID," + "journalSubfieldDepartment," + "journalSubfieldScore,"
        + "relationshipEvidenceTotalScore," + "relationshipMinimumTotalScore," + "relationshipNonMatchCount," + "relationshipNonMatchScore,"
        + "articleYear," + "identityBachelorYear," + "discrepancyDegreeYearBachelor," + "discrepancyDegreeYearBachelorScore," + "identityDoctoralYear,"
        + "discrepancyDegreeYearDoctoral," + "discrepancyDegreeYearDoctoralScore," + "genderScoreArticle," + "genderScoreIdentity," + "genderScoreIdentityArticleDiscrepancy,"
        + "personType," + "personTypeScore," + "countArticlesRetrieved," + "articleCountScore," 
        + "targetAuthorInstitutionalAffiliationArticlePubmedLabel," + "pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore," + "scopusNonTargetAuthorInstitutionalAffiliationSource," + "scopusNonTargetAuthorInstitutionalAffiliationScore,"
        + "totalArticleScoreWithoutClustering," + "clusterScoreAverage," + "clusterReliabilityScore," + "clusterScoreModificationOfTotalScore," 
        + "datePublicationAddedToEntrez," + "clusterIdentifier," + "doi," + "issn," + "issue," + "journalTitleISOabbreviation," + "pages," + "timesCited," + "volume"
        + "\n")
#use count to record the number of person we have finished feature extraction
count = 0
#extract all required nested features 

for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        personIdentifier = items[i]['reCiterFeature']['personIdentifier']
        pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']

        totalArticleScoreStandardized = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['totalArticleScoreStandardized']
        totalArticleScoreNonStandardized = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['totalArticleScoreNonStandardized']
        userAssertion = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['userAssertion']
        publicationDateStandardized = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['publicationDateStandardized']
        publicationTypeCanonical = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['publicationType']['publicationTypeCanonical']
        # example1: when you get key error, check whether the key exist in dynamodb or not
        if 'scopusDocID' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            scopusDocID = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['scopusDocID']
        else:
            scopusDocID = ""
        
        if 'pmcid' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            pmcid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmcid']
        else:
            pmcid = ""
        
        journalTitleVerbose = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['journalTitleVerbose']
        journalTitleVerbose = journalTitleVerbose.replace('"', '""')
        if 'articleTitle' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            articleTitle = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['articleTitle']
            articleTitle = articleTitle.replace('"', '""')
        else:
            articleTitle = ""

        if 'acceptedRejectedEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            if 'feedbackScoreAccepted' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                feedbackScoreAccepted = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreAccepted']
            else: 
                feedbackScoreAccepted = 0
            if 'feedbackScoreRejected' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                feedbackScoreRejected = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreRejected']
            else: 
                feedbackScoreRejected = 0
            if 'feedbackScoreNull' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']:
                feedbackScoreNull = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['acceptedRejectedEvidence']['feedbackScoreNull']
            else: 
                feedbackScoreNull = 0
        else:
            feedbackScoreAccepted, feedbackScoreRejected, feedbackScoreNull = "", "", ""
        
        if 'authorNameEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            if 'articleAuthorName' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                if 'firstName' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']: 
                    articleAuthorName_firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']['firstName']
                else:
                    articleAuthorName_firstName = ""
                articleAuthorName_lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['articleAuthorName']['lastName']
            else:
                articleAuthorName_firstName, articleAuthorName_lastName = "", ""
            institutionalAuthorName_firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['firstName']
            if 'middleName' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']:
                institutionalAuthorName_middleName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['middleName']
            else:
                institutionalAuthorName_middleName = ""
            institutionalAuthorName_lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['institutionalAuthorName']['lastName']
            nameMatchFirstScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstScore']
            if 'nameMatchFirstType' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                nameMatchFirstType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchFirstType']
            nameMatchMiddleScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleScore']
            if 'nameMatchMiddleType' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                nameMatchMiddleType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchMiddleType']
            nameMatchLastScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastScore']
            if 'nameMatchLastType' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']:
                nameMatchLastType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchLastType']
            nameMatchModifierScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameMatchModifierScore']
            nameScoreTotal = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['authorNameEvidence']['nameScoreTotal']
        else:
            articleAuthorName_firstName, articleAuthorName_lastName, institutionalAuthorName_firstName, institutionalAuthorName_middleName, institutionalAuthorName_lastName = "", "", "", "", ""
            nameMatchFirstScore, nameMatchFirstType, nameMatchMiddleScore, nameMatchMiddleType, nameMatchLastScore, nameMatchLastType, nameMatchModifierScore, nameScoreTotal = "", "", "", "", "", "", "", ""

        if 'emailEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            emailMatch = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatch']
            if 'false' in emailMatch:
                emailMatch = ""
            emailMatchScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['emailEvidence']['emailMatchScore']
        else:
            emailMatch, emailMatchScore = "", 0
        
        if 'journalCategoryEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            journalSubfieldScienceMetrixLabel = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixLabel']
            journalSubfieldScienceMetrixLabel = journalSubfieldScienceMetrixLabel.replace('"', '""')
            journalSubfieldScienceMetrixID = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScienceMetrixID']
            journalSubfieldDepartment = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldDepartment']
            journalSubfieldDepartment = journalSubfieldDepartment.replace('"', '""')
            journalSubfieldScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['journalCategoryEvidence']['journalSubfieldScore']
        else:
            journalSubfieldScienceMetrixLabel, journalSubfieldScienceMetrixID, journalSubfieldDepartment, journalSubfieldScore = "", "", "", 0
        
        if 'relationshipEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            relationshipEvidenceTotalScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipEvidenceTotalScore']
            if 'relationshipNegativeMatch' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']:
                relationshipMinimumTotalScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipMinimumTotalScore']
                relationshipNonMatchCount = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchCount']
                relationshipNonMatchScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipNegativeMatch']['relationshipNonMatchScore']
            else:
                relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = 0, 0, 0
        else:
            relationshipEvidenceTotalScore, relationshipMinimumTotalScore, relationshipNonMatchCount, relationshipNonMatchScore = 0, 0, 0, 0
        
        if 'educationYearEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            if 'articleYear' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                articleYear = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['articleYear']
            else:
                articleYear = 0
            if 'identityBachelorYear' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                identityBachelorYear = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityBachelorYear']
            else:
                identityBachelorYear = ""
            if 'discrepancyDegreeYearBachelor' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearBachelor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelor']
            else:
                discrepancyDegreeYearBachelor = 0
            if 'discrepancyDegreeYearBachelorScore' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearBachelorScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearBachelorScore']
            else:
                discrepancyDegreeYearBachelorScore = 0
            if 'identityDoctoralYear' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                identityDoctoralYear = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['identityDoctoralYear']
            else:
                identityDoctoralYear = ""
            if 'discrepancyDegreeYearDoctoral' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearDoctoral = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoral']
            else:
                discrepancyDegreeYearDoctoral = 0
            if 'discrepancyDegreeYearDoctoralScore' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']:
                discrepancyDegreeYearDoctoralScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['educationYearEvidence']['discrepancyDegreeYearDoctoralScore']
            else:
                discrepancyDegreeYearDoctoralScore = 0
        else:
            articleYear, identityBachelorYear, discrepancyDegreeYearBachelor, discrepancyDegreeYearBachelorScore, identityDoctoralYear, discrepancyDegreeYearDoctoral, discrepancyDegreeYearDoctoralScore = 0, "", 0, 0, "", 0, 0
        
        if 'genderEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            genderScoreArticle = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreArticle']
            genderScoreIdentity = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentity']
            genderScoreIdentityArticleDiscrepancy = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['genderEvidence']['genderScoreIdentityArticleDiscrepancy']
        else:
            genderScoreArticle, genderScoreIdentity, genderScoreIdentityArticleDiscrepancy = 0, 0, 0
        
        if 'personTypeEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            personType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personType']
            personTypeScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['personTypeEvidence']['personTypeScore']
        else:
            personType, personTypeScore = "", 0
        
        if 'articleCountEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            countArticlesRetrieved = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['countArticlesRetrieved']
            articleCountScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['articleCountEvidence']['articleCountScore']
        else:
            countArticlesRetrieved, articleCountScore = 0, 0
        
        if 'affiliationEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence'] and \
            'pubmedTargetAuthorAffiliation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            targetAuthorInstitutionalAffiliationArticlePubmedLabel = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationArticlePubmedLabel']
            targetAuthorInstitutionalAffiliationArticlePubmedLabel = targetAuthorInstitutionalAffiliationArticlePubmedLabel.replace('"', '""')
            pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['pubmedTargetAuthorAffiliation']['targetAuthorInstitutionalAffiliationMatchTypeScore']
        else:
            targetAuthorInstitutionalAffiliationArticlePubmedLabel, pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = "", 0

        if 'affiliationEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence'] and \
            'scopusNonTargetAuthorAffiliation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            scopusNonTargetAuthorInstitutionalAffiliationSource = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationSource']
            scopusNonTargetAuthorInstitutionalAffiliationScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationScore']
        else:
            scopusNonTargetAuthorInstitutionalAffiliationSource, scopusNonTargetAuthorInstitutionalAffiliationScore = "", 0

        if 'averageClusteringEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            totalArticleScoreWithoutClustering = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['totalArticleScoreWithoutClustering']
            clusterScoreAverage = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreAverage']
            clusterReliabilityScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterReliabilityScore']
            clusterScoreModificationOfTotalScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterScoreModificationOfTotalScore']
        else:
            totalArticleScoreWithoutClustering, clusterScoreAverage, clusterReliabilityScore, clusterScoreModificationOfTotalScore = "", "", "", ""
        if 'averageClusteringEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence'] and \
            'clusterIdentifier' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']:
            clusterIdentifier = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['averageClusteringEvidence']['clusterIdentifier']
        else :
            clusterIdentifier = 0

        if 'datePublicationAddedToEntrez' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            datePublicationAddedToEntrez = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['datePublicationAddedToEntrez']
        else:
            datePublicationAddedToEntrez = ""
        
        if 'publicationDateDisplay' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            publicationDateDisplay = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['publicationDateDisplay']
        else:
            publicationDateDisplay = ""

        if 'doi' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            doi = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['doi']
        else: 
            doi = ""

        if 'issn' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            issn_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issn'])
            for k in range(issn_temp):
                issntype = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issn'][k]['issntype']
                if issntype == 'Linking':
                    issn = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
                if issntype == 'Print':
                    issn = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
                if issntype == 'Electronic':
                    issn = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issn'][k]['issn']
                    break
        else:
            issn = ""

        if 'issue' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            issue = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['issue']
        else: 
            issue = ""
        if 'journalTitleISOabbreviation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            journalTitleISOabbreviation = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['journalTitleISOabbreviation']
            journalTitleISOabbreviation = journalTitleISOabbreviation.replace('"', '""')
        else:
            journalTitleISOabbreviation = ""
        if 'pages' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            pages = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pages']
        else:
            pages = ""
        if 'timesCited' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            timesCited = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['timesCited']
        else: 
            timesCited = 0
        if 'volume' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            volume = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['volume']
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
    print("here:", count)
f.close()

#### The logic of all parts below is similar to the first part, please refer to the first part for explaination ####
#code for person_article_grant table
f = open(outputPath + 'person_article_grant1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "articleGrant," + "grantMatchScore," + "institutionGrant" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'grantEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            grants_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'])
        
            for k in range(grants_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                articleGrant = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['articleGrant']
                grantMatchScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['grantMatchScore']
                institutionGrant = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['grantEvidence']['grants'][k]['institutionGrant']
    
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(articleGrant) + '"' + "," 
                    + str(grantMatchScore)  + "," + '"' + str(institutionGrant) + '"' + "\n")
    count += 1
    print("here:", count)
f.close()



#code for person_article_scopus_non_target_author_affiliation table
f = open(outputPath + 'person_article_scopus_non_target_author_affiliation1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "nonTargetAuthorInstitutionLabel," + "nonTargetAuthorInstitutionID," + "nonTargetAuthorInstitutionCount" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'affiliationEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence'] and \
        'scopusNonTargetAuthorAffiliation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence'] and \
        'nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']:
            scopusNonTargetAuthorAffiliation_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution'])
    
            for k in range(scopusNonTargetAuthorAffiliation_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusNonTargetAuthorAffiliation']['nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution'][k]
                #since the nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution field contains more than one featureseparated by comma, and string feature contains comma, we need to disdinguish between this two by the following code
                count_comma = nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution.count(',')
                comma_difference = count_comma - 2
                if comma_difference != 0:
                    nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution = nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution.replace(",", ".", comma_difference)
                f.write(str(personIdentifier) + "," + str(pmid) + "," + str(nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for person_article_scopus_target_author_affiliation table
f = open(outputPath + 'person_article_scopus_target_author_affiliation1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "targetAuthorInstitutionalAffiliationSource," + "scopusTargetAuthorInstitutionalAffiliationIdentity," + "targetAuthorInstitutionalAffiliationArticleScopusLabel,"
        + "targetAuthorInstitutionalAffiliationArticleScopusAffiliationId," + "targetAuthorInstitutionalAffiliationMatchType," + "targetAuthorInstitutionalAffiliationMatchTypeScore" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'affiliationEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence'] and 'scopusTargetAuthorAffiliation' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']:
            scopusTargetAuthorAffiliation_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'])
        
            for k in range(scopusTargetAuthorAffiliation_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                targetAuthorInstitutionalAffiliationSource = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationSource']
                if 'scopusTargetAuthorInstitutionalAffiliationIdentity' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]:
                    scopusTargetAuthorInstitutionalAffiliationIdentity = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationIdentity']
                else:
                    scopusTargetAuthorInstitutionalAffiliationIdentity = ""
                if 'targetAuthorInstitutionalAffiliationArticleScopusLabel' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]:
                    targetAuthorInstitutionalAffiliationArticleScopusLabel = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationArticleScopusLabel']
                else:
                    targetAuthorInstitutionalAffiliationArticleScopusLabel = ""
                targetAuthorInstitutionalAffiliationArticleScopusAffiliationId = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationArticleScopusAffiliationId']
                targetAuthorInstitutionalAffiliationMatchType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationMatchType']
                targetAuthorInstitutionalAffiliationMatchTypeScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['affiliationEvidence']['scopusTargetAuthorAffiliation'][k]['targetAuthorInstitutionalAffiliationMatchTypeScore']

                f.write(str(personIdentifier) + "," + str(pmid) + "," + str(targetAuthorInstitutionalAffiliationSource) + "," 
                    + '"' + str(scopusTargetAuthorInstitutionalAffiliationIdentity) + '"' + "," + '"' + str(targetAuthorInstitutionalAffiliationArticleScopusLabel) + '"' + "," + str(targetAuthorInstitutionalAffiliationArticleScopusAffiliationId) + "," 
                    + str(targetAuthorInstitutionalAffiliationMatchType) + "," + str(targetAuthorInstitutionalAffiliationMatchTypeScore) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for person_article_department table
f = open(outputPath + 'person_article_department1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "identityOrganizationalUnit," + "articleAffiliation," 
        + "organizationalUnitType," + "organizationalUnitMatchingScore," + "organizationalUnitModifier," + "organizationalUnitModifierScore" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'organizationalUnitEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            organizationalUnit_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'])
        
            for k in range(organizationalUnit_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                identityOrganizationalUnit = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['identityOrganizationalUnit']
                identityOrganizationalUnit = identityOrganizationalUnit.replace('"', '""')
                articleAffiliation = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['articleAffiliation']
                articleAffiliation = articleAffiliation.replace('"', '""')
                organizationalUnitType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitType']
                organizationalUnitMatchingScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitMatchingScore']
                if 'organizationalUnitModifier' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]:
                    organizationalUnitModifier = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitModifier']
                else:
                    organizationalUnitModifier = ""
                organizationalUnitModifierScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['organizationalUnitEvidence'][k]['organizationalUnitModifierScore']
                
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(identityOrganizationalUnit) + '"' + "," 
                    + '"' + str(articleAffiliation) + '"' + "," + str(organizationalUnitType) + "," 
                    + str(organizationalUnitMatchingScore) + "," + str(organizationalUnitModifier) + "," + str(organizationalUnitModifierScore) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for person_article_relationship table
f = open(outputPath + 'person_article_relationship1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "relationshipNameArticleFirstName," + "relationshipNameArticleLastName," 
        + "relationshipNameIdentityFirstName," + "relationshipNameIdentityLastName," + "relationshipType," + "relationshipMatchType,"
        + "relationshipMatchingScore," + "relationshipVerboseMatchModifierScore," + "relationshipMatchModifierMentor,"
        + "relationshipMatchModifierMentorSeniorAuthor," + "relationshipMatchModifierManager," + "relationshipMatchModifierManagerSeniorAuthor" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        if 'relationshipEvidence' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']:
            relationshipPositiveMatch_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'])
        
            for k in range(relationshipPositiveMatch_temp):
                personIdentifier = items[i]['reCiterFeature']['personIdentifier']
                pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
                relationshipNameArticle_firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameArticle']['firstName']
                relationshipNameArticle_lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameArticle']['lastName']
                relationshipNameIdentity_firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdentity']['firstName']
                relationshipNameIdentity_lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipNameIdentity']['lastName']
                relationshipType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipType']
                relationshipMatchType = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchType'] 
                relationshipMatchingScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchingScore']
                relationshipVerboseMatchModifierScore = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipVerboseMatchModifierScore']
                relationshipMatchModifierMentor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierMentor']
                relationshipMatchModifierMentorSeniorAuthor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierMentorSeniorAuthor']
                relationshipMatchModifierManager = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierManager']
                relationshipMatchModifierManagerSeniorAuthor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['evidence']['relationshipEvidence']['relationshipPositiveMatch'][k]['relationshipMatchModifierManagerSeniorAuthor']
                
                f.write(str(personIdentifier) + "," + str(pmid) + "," + str(relationshipNameArticle_firstName) + "," 
                    + str(relationshipNameArticle_lastName) + "," + str(relationshipNameIdentity_firstName) + "," 
                    + str(relationshipNameIdentity_lastName) + "," + '"' + str(relationshipType) + '"' + "," + str(relationshipMatchType) + ","
                    + str(relationshipMatchingScore) + "," + str(relationshipVerboseMatchModifierScore) + "," + str(relationshipMatchModifierMentor) + ","
                    + str(relationshipMatchModifierMentorSeniorAuthor) + "," + str(relationshipMatchModifierManager) + "," + str(relationshipMatchModifierManagerSeniorAuthor) + "\n")
    count += 1
    print("here:", count)
f.close() 



#code for person table
f = open(outputPath + 'person1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "dateAdded," + "dateUpdated," + "precision," + "recall," + "countSuggestedArticles," + "countPendingArticles," + "overallAccuracy," + "mode" + "\n")

count = 0
for i in range(len(items)):
    personIdentifier = items[i]['reCiterFeature']['personIdentifier']
    dateAdded = items[i]['reCiterFeature']['dateAdded']
    dateUpdated = items[i]['reCiterFeature']['dateUpdated']
    precision = items[i]['reCiterFeature']['precision']
    recall = items[i]['reCiterFeature']['recall']
    countSuggestedArticles = items[i]['reCiterFeature']['countSuggestedArticles']
    if 'countPendingArticles' in items[i]['reCiterFeature']:
        countPendingArticles = items[i]['reCiterFeature']['countPendingArticles']
    else:
        countPendingArticles = 0
    overallAccuracy = items[i]['reCiterFeature']['overallAccuracy']
    mode = items[i]['reCiterFeature']['mode']
    
    f.write(str(personIdentifier) + "," + str(dateAdded) + "," + str(dateUpdated) + "," 
                + str(precision) + "," + str(recall) + "," 
                + str(countSuggestedArticles) + "," + str(countPendingArticles) + "," + str(overallAccuracy) + "," + str(mode) + "\n")
    count += 1
    print("here:", count)
f.close()



#code for person_article_author table

#record a articles associated number of authors
count_authors_dict = {}
for i in range(len(items)):
    temp = count_articles[i][1]
    for j in range(temp):
        count_authors_dict[str(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid'])] =  len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'])
print(len(count_authors_dict))

f = open(outputPath + 'person_article_author1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "authorFirstName," + "authorLastName," + "targetAuthor," + "rank," + "orcid," + "equalContrib" + "\n")

count = 0
for i in range(len(items)):
    article_temp = count_articles[i][1]
    
    for j in range(article_temp):
        pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
        personIdentifier = items[i]['reCiterFeature']['personIdentifier']
        author_temp = count_authors_dict[str(pmid)]
        for k in range(author_temp):
            try:
                if 'firstName' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
                    firstName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['firstName']
                else:
                    firstName = ""
                lastName = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['lastName']
                targetAuthor = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['targetAuthor']
                rank = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['rank']
                if 'orcid' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
                    orcid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['orcid']
                else:
                    orcid = ""
                if 'equalContrib' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]:
                    equalContrib = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['reCiterArticleAuthorFeatures'][k]['equalContrib']
                else:
                    equalContrib = ""                    
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(firstName) + '"' + "," + '"' + str(lastName) + '"' + "," + str(targetAuthor) + "," + str(rank) + "," + str(orcid) + "," + str(equalContrib) + "\n")
            except IndexError:
                firstName = ""
                lastName = ""
                targetAuthor = ""
                rank = 0
                orcid = ""
                equalContrib = ""
    count += 1
    print("here:", count)
f.close()


#code for person_article_keyword table
#open a csv file in the directory you preferred
f = open(outputPath + 'person_article_keyword1.csv','w', encoding='utf-8')
#write column names into file
f.write("personIdentifier," + "pmid," + "keyword" + "\n")
#use count to record the number of person we have finished feature extraction
count = 0
#extract all required nested features 

for i in range(len(items)):
    article_temp = count_articles[i][1]
    for j in range(article_temp):
        personIdentifier = items[i]['reCiterFeature']['personIdentifier']
        pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
        if 'articleKeywords' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]:
            keywords_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['articleKeywords'])
            for k in range(keywords_temp):
                if 'keyword' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['articleKeywords'][k]:
                    keyword = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['articleKeywords'][k]['keyword']
                else:
                    keyword = ""
                f.write(str(personIdentifier) + "," + str(pmid) + "," + '"' + str(keyword) + '"' + "\n")
            
f.close()
