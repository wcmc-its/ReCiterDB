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

    '''
    # UIDs to filter
    uids_to_retrieve = ['meb7002', 'ccole']

    response = table.scan(
        FilterExpression = Attr('usingS3').eq(0) & Attr('uid').is_in(uids_to_retrieve),
    )
    '''

    response = table.scan(
        FilterExpression = Attr('usingS3').eq(0),
    )

    items = response['Items']

    #continue to get all records in the table, using ExclusiveStartKey
    while True:
        print(len(response['Items']))
        if response.get('LastEvaluatedKey'):
            if is_filter_expression: 
                response = table.scan(
                    ExclusiveStartKey = response['LastEvaluatedKey'],
                    FilterExpression = Attr('usingS3').eq(0) # & Attr('uid').is_in(uids_to_retrieve)
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



# Open a CSV file in the directory you preferred
with open(os.path.join(outputPath, 'person_article1.csv'), 'w', encoding='utf-8') as f:
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
        personIdentifier = item['reCiterFeature']['personIdentifier']
        article_features = item['reCiterFeature'].get('reCiterArticleFeatures', [])
        article_count = len(article_features)

        for article in article_features:
            pmid = article.get('pmid', "")
            # Determine if the record is in new or old format
            is_new_format = 'authorshipLikelihoodScore' in article

            # Common fields
            pmcid = article.get('pmcid', "")
            totalArticleScoreStandardized = article.get('totalArticleScoreStandardized', "")
            totalArticleScoreNonStandardized = article.get('totalArticleScoreNonStandardized', "")
            userAssertion = article.get('userAssertion', "")
            publicationDateDisplay = article.get('publicationDateDisplay', "")
            publicationDateStandardized = article.get('publicationDateStandardized', "")
            publicationTypeCanonical = article.get('publicationType', {}).get('publicationTypeCanonical', "")
            scopusDocID = article.get('scopusDocID', "")
            journalTitleVerbose = article.get('journalTitleVerbose', "").replace('"', '""')
            articleTitle = article.get('articleTitle', "").replace('"', '""')

            # New fields (only in new format)
            if is_new_format:
                authorshipLikelihoodScore = article.get('authorshipLikelihoodScore', "")
            else:
                authorshipLikelihoodScore = ""

            # Initialize variables to default values before attempting to assign them
            articleAuthorName_firstName = ""
            articleAuthorName_lastName = ""
            institutionalAuthorName_firstName = ""
            institutionalAuthorName_middleName = ""
            institutionalAuthorName_lastName = ""
            nameMatchFirstScore = ""
            nameMatchFirstType = ""
            nameMatchMiddleScore = ""
            nameMatchMiddleType = ""
            nameMatchLastScore = ""
            nameMatchLastType = ""
            nameMatchModifierScore = ""
            nameScoreTotal = ""
            # Initialize other variables similarly...

            # Accepted/Rejected Evidence
            evidence = article.get('evidence', {})
            acceptedRejectedEvidence = evidence.get('acceptedRejectedEvidence', {})
            feedbackScoreAccepted = acceptedRejectedEvidence.get('feedbackScoreAccepted', "")
            feedbackScoreRejected = acceptedRejectedEvidence.get('feedbackScoreRejected', "")
            feedbackScoreNull = acceptedRejectedEvidence.get('feedbackScoreNull', "")

            # Feedback Evidence (New attributes)
            feedbackEvidence = evidence.get('feedbackEvidence', {})
            feedbackScoreCites = feedbackEvidence.get('feedbackScoreCites', "")
            feedbackScoreCoAuthorName = feedbackEvidence.get('feedbackScoreCoAuthorName', "")
            feedbackScoreEmail = feedbackEvidence.get('feedbackScoreEmail', "")
            feedbackScoreInstitution = feedbackEvidence.get('feedbackScoreInstitution', "")
            feedbackScoreJournal = feedbackEvidence.get('feedbackScoreJournal', "")
            feedbackScoreJournalSubField = feedbackEvidence.get('feedbackScoreJournalSubField', "")
            feedbackScoreKeyword = feedbackEvidence.get('feedbackScoreKeyword', "")
            feedbackScoreOrcid = feedbackEvidence.get('feedbackScoreOrcid', "")
            feedbackScoreOrcidCoAuthor = feedbackEvidence.get('feedbackScoreOrcidCoAuthor', "")
            feedbackScoreOrganization = feedbackEvidence.get('feedbackScoreOrganization', "")
            feedbackScoreTargetAuthorName = feedbackEvidence.get('feedbackScoreTargetAuthorName', "")
            feedbackScoreYear = feedbackEvidence.get('feedbackScoreYear', "")

            # Author Name Evidence
            authorNameEvidence = evidence.get('authorNameEvidence', {})
            if authorNameEvidence:
                articleAuthorName = authorNameEvidence.get('articleAuthorName', {})
                articleAuthorName_firstName = articleAuthorName.get('firstName', "")
                articleAuthorName_lastName = articleAuthorName.get('lastName', "")
                institutionalAuthorName = authorNameEvidence.get('institutionalAuthorName', {})
                institutionalAuthorName_firstName = institutionalAuthorName.get('firstName', "")
                institutionalAuthorName_middleName = institutionalAuthorName.get('middleName', "")
                institutionalAuthorName_lastName = institutionalAuthorName.get('lastName', "")
                nameMatchFirstScore = authorNameEvidence.get('nameMatchFirstScore', "")
                nameMatchFirstType = authorNameEvidence.get('nameMatchFirstType', "")
                nameMatchMiddleScore = authorNameEvidence.get('nameMatchMiddleScore', "")
                nameMatchMiddleType = authorNameEvidence.get('nameMatchMiddleType', "")
                nameMatchLastScore = authorNameEvidence.get('nameMatchLastScore', "")
                nameMatchLastType = authorNameEvidence.get('nameMatchLastType', "")
                nameMatchModifierScore = authorNameEvidence.get('nameMatchModifierScore', "")
                nameScoreTotal = authorNameEvidence.get('nameScoreTotal', "")
            else:
                # Variables have already been initialized to default empty strings
                pass

            # Email Evidence
            emailEvidence = evidence.get('emailEvidence', {})
            emailMatch = emailEvidence.get('emailMatch', "")
            emailMatchScore = emailEvidence.get('emailMatchScore', "")

            # Journal Category Evidence
            journalCategoryEvidence = evidence.get('journalCategoryEvidence', {})
            journalSubfieldScienceMetrixLabel = journalCategoryEvidence.get('journalSubfieldScienceMetrixLabel', "").replace('"', '""')
            journalSubfieldScienceMetrixID = journalCategoryEvidence.get('journalSubfieldScienceMetrixID', "")
            journalSubfieldDepartment = journalCategoryEvidence.get('journalSubfieldDepartment', "").replace('"', '""')
            journalSubfieldScore = journalCategoryEvidence.get('journalSubfieldScore', "")

            # Relationship Evidence
            relationshipEvidence = evidence.get('relationshipEvidence', {})
            relationshipEvidenceTotalScore = relationshipEvidence.get('relationshipEvidenceTotalScore', "")
            relationshipNegativeMatch = relationshipEvidence.get('relationshipNegativeMatch', {})
            relationshipMinimumTotalScore = relationshipNegativeMatch.get('relationshipMinimumTotalScore', "")
            relationshipNonMatchCount = relationshipNegativeMatch.get('relationshipNonMatchCount', "")
            relationshipNonMatchScore = relationshipNegativeMatch.get('relationshipNonMatchScore', "")

            # Education Year Evidence
            educationYearEvidence = evidence.get('educationYearEvidence', {})
            articleYear = educationYearEvidence.get('articleYear', "")
            identityBachelorYear = educationYearEvidence.get('identityBachelorYear', "")
            discrepancyDegreeYearBachelor = educationYearEvidence.get('discrepancyDegreeYearBachelor', "")
            discrepancyDegreeYearBachelorScore = educationYearEvidence.get('discrepancyDegreeYearBachelorScore', "")
            identityDoctoralYear = educationYearEvidence.get('identityDoctoralYear', "")
            discrepancyDegreeYearDoctoral = educationYearEvidence.get('discrepancyDegreeYearDoctoral', "")
            discrepancyDegreeYearDoctoralScore = educationYearEvidence.get('discrepancyDegreeYearDoctoralScore', "")

            # Gender Evidence
            genderEvidence = evidence.get('genderEvidence', {})
            genderScoreArticle = genderEvidence.get('genderScoreArticle', "")
            genderScoreIdentity = genderEvidence.get('genderScoreIdentity', "")
            genderScoreIdentityArticleDiscrepancy = genderEvidence.get('genderScoreIdentityArticleDiscrepancy', "")

            # Person Type Evidence
            personTypeEvidence = evidence.get('personTypeEvidence', {})
            personType = personTypeEvidence.get('personType', "")
            personTypeScore = personTypeEvidence.get('personTypeScore', "")

            # Article Count Evidence
            articleCountEvidence = evidence.get('articleCountEvidence', {})
            countArticlesRetrieved = articleCountEvidence.get('countArticlesRetrieved', "")
            articleCountScore = articleCountEvidence.get('articleCountScore', "")

            # Affiliation Evidence
            affiliationEvidence = evidence.get('affiliationEvidence', {})
            pubmedTargetAuthorAffiliation = affiliationEvidence.get('pubmedTargetAuthorAffiliation', {})
            targetAuthorInstitutionalAffiliationArticlePubmedLabel = pubmedTargetAuthorAffiliation.get('targetAuthorInstitutionalAffiliationArticlePubmedLabel', "").replace('"', '""')
            pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore = pubmedTargetAuthorAffiliation.get('targetAuthorInstitutionalAffiliationMatchTypeScore', "")
            scopusNonTargetAuthorAffiliation = affiliationEvidence.get('scopusNonTargetAuthorAffiliation', {})
            scopusNonTargetAuthorInstitutionalAffiliationSource = scopusNonTargetAuthorAffiliation.get('nonTargetAuthorInstitutionalAffiliationSource', "")
            scopusNonTargetAuthorInstitutionalAffiliationScore = scopusNonTargetAuthorAffiliation.get('nonTargetAuthorInstitutionalAffiliationScore', "")

            # Clustering Evidence
            averageClusteringEvidence = evidence.get('averageClusteringEvidence', {})
            totalArticleScoreWithoutClustering = averageClusteringEvidence.get('totalArticleScoreWithoutClustering', "")
            clusterScoreAverage = averageClusteringEvidence.get('clusterScoreAverage', "")
            clusterReliabilityScore = averageClusteringEvidence.get('clusterReliabilityScore', "")
            clusterScoreModificationOfTotalScore = averageClusteringEvidence.get('clusterScoreModificationOfTotalScore', "")
            clusterIdentifier = averageClusteringEvidence.get('clusterIdentifier', "")

            # Additional fields
            datePublicationAddedToEntrez = article.get('datePublicationAddedToEntrez', "")
            doi = article.get('doi', "")
            issn_list = article.get('issn', [])
            issn = ""
            for issn_item in issn_list:
                issn_type = issn_item.get('issntype', "")
                if issn_type in ['Linking', 'Print', 'Electronic']:
                    issn = issn_item.get('issn', "")
                    break
            issue = article.get('issue', "")
            journalTitleISOabbreviation = article.get('journalTitleISOabbreviation', "").replace('"', '""')
            pages = article.get('pages', "")
            timesCited = article.get('timesCited', "")
            volume = article.get('volume', "")

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

        count += 1
        print(f"Processed person {personIdentifier}: {article_count} articles")

print("Finished generating person_article1.csv")




#### The logic of all parts below is similar to the first part, please refer to the first part for explaination ####


# Code for person_article_grant table
f = open(outputPath + 'person_article_grant1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "articleGrant," + "grantMatchScore," + "institutionGrant" + "\n")

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'])
    for j in range(article_temp):
        grants = items[i]['reCiterFeature']['reCiterArticleFeatures'][j].get('evidence', {}).get('grantEvidence', {}).get('grants', [])
        for grant in grants:
            personIdentifier = items[i]['reCiterFeature']['personIdentifier']
            pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
            articleGrant = grant.get('articleGrant', "")
            grantMatchScore = grant.get('grantMatchScore', "")
            institutionGrant = grant.get('institutionGrant', "")
            f.write(f'{personIdentifier},{pmid},"{articleGrant}",{grantMatchScore},"{institutionGrant}"\n')
    count += 1
    print("Processed person:", count)
f.close()



# Code for person_article_scopus_non_target_author_affiliation table
f = open(outputPath + 'person_article_scopus_non_target_author_affiliation1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "nonTargetAuthorInstitutionLabel," + "nonTargetAuthorInstitutionID," + "nonTargetAuthorInstitutionCount" + "\n")

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'])
    for j in range(article_temp):
        non_target_affiliations = items[i]['reCiterFeature']['reCiterArticleFeatures'][j].get('evidence', {}).get('affiliationEvidence', {}).get('scopusNonTargetAuthorAffiliation', {}).get('nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution', [])
        for affiliation in non_target_affiliations:
            personIdentifier = items[i]['reCiterFeature']['personIdentifier']
            pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
            # Assuming the affiliation string is in the format "Label, ID, Count"
            f.write(f'{personIdentifier},{pmid},{affiliation}\n')
    count += 1
    print("Processed person:", count)
f.close()




# Code for person_article_scopus_target_author_affiliation table
f = open(outputPath + 'person_article_scopus_target_author_affiliation1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "targetAuthorInstitutionalAffiliationSource," + "scopusTargetAuthorInstitutionalAffiliationIdentity," + "targetAuthorInstitutionalAffiliationArticleScopusLabel,"
        + "targetAuthorInstitutionalAffiliationArticleScopusAffiliationId," + "targetAuthorInstitutionalAffiliationMatchType," + "targetAuthorInstitutionalAffiliationMatchTypeScore" + "\n")

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'])
    for j in range(article_temp):
        scopus_affiliations = items[i]['reCiterFeature']['reCiterArticleFeatures'][j].get('evidence', {}).get('affiliationEvidence', {}).get('scopusTargetAuthorAffiliation', [])
        for affiliation in scopus_affiliations:
            personIdentifier = items[i]['reCiterFeature']['personIdentifier']
            pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
            targetAuthorInstitutionalAffiliationSource = affiliation.get('targetAuthorInstitutionalAffiliationSource', "")
            scopusTargetAuthorInstitutionalAffiliationIdentity = affiliation.get('targetAuthorInstitutionalAffiliationIdentity', "")
            targetAuthorInstitutionalAffiliationArticleScopusLabel = affiliation.get('targetAuthorInstitutionalAffiliationArticleScopusLabel', "")
            targetAuthorInstitutionalAffiliationArticleScopusAffiliationId = affiliation.get('targetAuthorInstitutionalAffiliationArticleScopusAffiliationId', "")
            targetAuthorInstitutionalAffiliationMatchType = affiliation.get('targetAuthorInstitutionalAffiliationMatchType', "")
            targetAuthorInstitutionalAffiliationMatchTypeScore = affiliation.get('targetAuthorInstitutionalAffiliationMatchTypeScore', "")
            f.write(f'{personIdentifier},{pmid},{targetAuthorInstitutionalAffiliationSource},"{scopusTargetAuthorInstitutionalAffiliationIdentity}","{targetAuthorInstitutionalAffiliationArticleScopusLabel}",{targetAuthorInstitutionalAffiliationArticleScopusAffiliationId},{targetAuthorInstitutionalAffiliationMatchType},{targetAuthorInstitutionalAffiliationMatchTypeScore}\n')
    count += 1
    print("Processed person:", count)
f.close()



# Code for person_article_department table
f = open(outputPath + 'person_article_department1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "identityOrganizationalUnit," + "articleAffiliation," 
        + "organizationalUnitType," + "organizationalUnitMatchingScore," + "organizationalUnitModifier," + "organizationalUnitModifierScore" + "\n")

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'])
    for j in range(article_temp):
        # Determine if the record is in new or old format
        is_new_format = 'authorshipLikelihoodScore' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]

        organizationalUnitEvidence = items[i]['reCiterFeature']['reCiterArticleFeatures'][j].get('evidence', {}).get('organizationalUnitEvidence', [])

        for org_unit in organizationalUnitEvidence:
            personIdentifier = items[i]['reCiterFeature']['personIdentifier']
            pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
            identityOrganizationalUnit = org_unit.get('identityOrganizationalUnit', "").replace('"', '""')
            articleAffiliation = org_unit.get('articleAffiliation', "").replace('"', '""')
            organizationalUnitType = org_unit.get('organizationalUnitType', "")
            organizationalUnitMatchingScore = org_unit.get('organizationalUnitMatchingScore', "")
            organizationalUnitModifier = org_unit.get('organizationalUnitModifier', "")
            organizationalUnitModifierScore = org_unit.get('organizationalUnitModifierScore', "")

            f.write(f'"{personIdentifier}",{pmid},"{identityOrganizationalUnit}","{articleAffiliation}",{organizationalUnitType},{organizationalUnitMatchingScore},{organizationalUnitModifier},{organizationalUnitModifierScore}\n')
    count += 1
    print("Processed person:", count)
f.close()



# Code for person_article_relationship table
f = open(outputPath + 'person_article_relationship1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "relationshipNameArticleFirstName," + "relationshipNameArticleLastName," 
        + "relationshipNameIdentityFirstName," + "relationshipNameIdentityLastName," + "relationshipType," + "relationshipMatchType,"
        + "relationshipMatchingScore," + "relationshipVerboseMatchModifierScore," + "relationshipMatchModifierMentor,"
        + "relationshipMatchModifierMentorSeniorAuthor," + "relationshipMatchModifierManager," + "relationshipMatchModifierManagerSeniorAuthor" + "\n")

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'])
    for j in range(article_temp):
        is_new_format = 'authorshipLikelihoodScore' in items[i]['reCiterFeature']['reCiterArticleFeatures'][j]

        relationshipPositiveMatch = items[i]['reCiterFeature']['reCiterArticleFeatures'][j].get('evidence', {}).get('relationshipEvidence', {}).get('relationshipPositiveMatch', [])

        for relation in relationshipPositiveMatch:
            personIdentifier = items[i]['reCiterFeature']['personIdentifier']
            pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
            relationshipNameArticle_firstName = relation.get('relationshipNameArticle', {}).get('firstName', "")
            relationshipNameArticle_lastName = relation.get('relationshipNameArticle', {}).get('lastName', "")
            relationshipNameIdentity_firstName = relation.get('relationshipNameIdentity', {}).get('firstName', "")
            relationshipNameIdentity_lastName = relation.get('relationshipNameIdentity', {}).get('lastName', "")
            relationshipType = relation.get('relationshipType', "")
            relationshipMatchType = relation.get('relationshipMatchType', "")
            relationshipMatchingScore = relation.get('relationshipMatchingScore', "")
            relationshipVerboseMatchModifierScore = relation.get('relationshipVerboseMatchModifierScore', "")
            relationshipMatchModifierMentor = relation.get('relationshipMatchModifierMentor', "")
            relationshipMatchModifierMentorSeniorAuthor = relation.get('relationshipMatchModifierMentorSeniorAuthor', "")
            relationshipMatchModifierManager = relation.get('relationshipMatchModifierManager', "")
            relationshipMatchModifierManagerSeniorAuthor = relation.get('relationshipMatchModifierManagerSeniorAuthor', "")

            f.write(f'{personIdentifier},{pmid},"{relationshipNameArticle_firstName}","{relationshipNameArticle_lastName}","{relationshipNameIdentity_firstName}","{relationshipNameIdentity_lastName}","{relationshipType}",{relationshipMatchType},{relationshipMatchingScore},{relationshipVerboseMatchModifierScore},{relationshipMatchModifierMentor},{relationshipMatchModifierMentorSeniorAuthor},{relationshipMatchModifierManager},{relationshipMatchModifierManagerSeniorAuthor}\n')
    count += 1
    print("Processed person:", count)
f.close()



# Code for person table
f = open(outputPath + 'person1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "dateAdded," + "dateUpdated," + "precision," + "recall," + "countSuggestedArticles," + "countPendingArticles," + "overallAccuracy," + "mode" + "\n")

count = 0
for i in range(len(items)):
    personIdentifier = items[i]['reCiterFeature']['personIdentifier']
    dateAdded = items[i]['reCiterFeature'].get('dateAdded', "")
    dateUpdated = items[i]['reCiterFeature'].get('dateUpdated', "")
    precision = items[i]['reCiterFeature'].get('precision', "")
    recall = items[i]['reCiterFeature'].get('recall', "")
    countSuggestedArticles = items[i]['reCiterFeature'].get('countSuggestedArticles', "")
    countPendingArticles = items[i]['reCiterFeature'].get('countPendingArticles', "")
    overallAccuracy = items[i]['reCiterFeature'].get('overallAccuracy', "")
    mode = items[i]['reCiterFeature'].get('mode', "")

    f.write(f'{personIdentifier},{dateAdded},{dateUpdated},{precision},{recall},{countSuggestedArticles},{countPendingArticles},{overallAccuracy},{mode}\n')
    count += 1
    print("Processed person:", count)
f.close()



# Code for person_article_author table

# Record articles associated with the number of authors
count_authors_dict = {}
for i in range(len(items)):
    article_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'])
    for j in range(article_temp):
        pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
        count_authors = len(items[i]['reCiterFeature']['reCiterArticleFeatures'][j].get('reCiterArticleAuthorFeatures', []))
        count_authors_dict[str(pmid)] = count_authors

f = open(outputPath + 'person_article_author1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "authorFirstName," + "authorLastName," + "targetAuthor," + "rank," + "orcid," + "equalContrib" + "\n")

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'])
    for j in range(article_temp):
        pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
        personIdentifier = items[i]['reCiterFeature']['personIdentifier']
        author_features = items[i]['reCiterFeature']['reCiterArticleFeatures'][j].get('reCiterArticleAuthorFeatures', [])
        for author in author_features:
            firstName = author.get('firstName', "")
            lastName = author.get('lastName', "")
            targetAuthor = author.get('targetAuthor', "")
            rank = author.get('rank', "")
            orcid = author.get('orcid', "")
            equalContrib = author.get('equalContrib', "")
            f.write(f'{personIdentifier},{pmid},"{firstName}","{lastName}",{targetAuthor},{rank},"{orcid}","{equalContrib}"\n')
    count += 1
    print("Processed person:", count)
f.close()


# Code for person_article_keyword table
f = open(outputPath + 'person_article_keyword1.csv','w', encoding='utf-8')
f.write("personIdentifier," + "pmid," + "keyword" + "\n")

count = 0
for i in range(len(items)):
    article_temp = len(items[i]['reCiterFeature']['reCiterArticleFeatures'])
    for j in range(article_temp):
        personIdentifier = items[i]['reCiterFeature']['personIdentifier']
        pmid = items[i]['reCiterFeature']['reCiterArticleFeatures'][j]['pmid']
        articleKeywords = items[i]['reCiterFeature']['reCiterArticleFeatures'][j].get('articleKeywords', [])
        for keyword_entry in articleKeywords:
            keyword = keyword_entry.get('keyword', "")
            f.write(f'{personIdentifier},{pmid},"{keyword}"\n')
    count += 1
    print("Processed person:", count)
f.close()
