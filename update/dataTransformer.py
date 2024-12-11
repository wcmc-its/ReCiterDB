# data_transformer.py

import os
import csv
import json
from datetime import datetime, timezone
import time

error_log_file = 'error.txt'

def log_error(person_identifier, error_message):
    """Log errors to the error.txt file."""
    with open(error_log_file, 'a') as f:
        f.write(f"PersonIdentifier: {person_identifier}, Error: {error_message}\n")

def sanitize_field(value):
    """Sanitize field by handling special characters for CSV."""
    if value is None or value == 'NULL' or (isinstance(value, str) and value.strip() == ''):
        return ''  # Blank for null or empty fields
    # Convert non-string values to strings
    value = str(value)
    # Remove unwanted characters
    value = value.replace('\r', '').replace('\n', '')
    # value = value.replace('\r', '').replace('\n', '').replace('"', '""')  # Escape double quotes    
    return value.strip()

def write_csv_header(file_path, headers):
    """Write the header row to a CSV file."""
    with open(file_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

def write_csv_rows(file_path, rows):
    """
    Append rows to a CSV file using csv.writer for proper formatting and consistent line endings.

    Args:
        file_path (str): Path to the CSV file.
        rows (list): List of rows to write.
    """
    with open(file_path, 'a', encoding='utf-8', newline='\n') as f:  # Force Unix-style line endings
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            # Ensure no unwanted line breaks or extra characters in any field
            sanitized_row = [sanitize_field(value) for value in row]
            writer.writerow(sanitized_row)

def convert_timestamp(timestamp):
    """Convert Unix timestamp in milliseconds to ISO 8601 format."""
    if isinstance(timestamp, (int, float)):
        # Convert milliseconds to seconds
        timestamp /= 1000.0
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.isoformat(timespec='milliseconds')
    return timestamp  # Return as-is if not a number

def process_person_temp(identities, output_path):
    """
    Process identities and write to person_temp.csv, ensuring Unix-style line endings.
    """
    output_file = os.path.join(output_path, 'person_temp.csv')
    preferred_domains = [
        "@med.cornell.edu", "@qatar-med.cornell.edu", "@nyp.org",
        "@hss.edu", "@mskcc.org", "@rockefeller.edu"
    ]
    headers = [
        "lastName", "title", "firstName", "middleName",
        "primaryEmail", "primaryOrganizationalUnit", "primaryInstitution", "personIdentifier", "knownRelationshipCount"
    ]
    try:
        write_csv_header(output_file, headers)
        rows = []
        for identity in identities:
            try:
                person_identifier = sanitize_field(identity.get('uid', ''))
                identity_data = identity.get('identity', {})
                primary_name = identity_data.get('primaryName', {})
                first_name = sanitize_field(primary_name.get('firstName', ''))
                middle_name = sanitize_field(primary_name.get('middleName', ''))
                last_name = sanitize_field(primary_name.get('lastName', ''))
                title = sanitize_field(identity_data.get('title', ''))
                emails = identity_data.get('emails', [])
                sanitized_emails = [sanitize_field(email.split(",")[0].strip()) for email in emails]
                primary_email = None
                for domain in preferred_domains:
                    for email in sanitized_emails:
                        if domain in email:
                            primary_email = email
                            break
                    if primary_email:
                        break
                primary_email = primary_email or (sanitized_emails[0] if sanitized_emails else "")
                primary_organizational_unit = sanitize_field(identity_data.get('primaryOrganizationalUnit', ''))
                primary_institution = sanitize_field(identity_data.get('primaryInstitution', ''))

                # Count the number of "uid" in knownRelationships
                known_relationships = identity_data.get('knownRelationships', [])
                relationshipIdentityCount = sum(1 for rel in known_relationships if rel.get('uid'))

                rows.append([
                    sanitize_field(last_name), sanitize_field(title), sanitize_field(first_name),
                    sanitize_field(middle_name), sanitize_field(primary_email),
                    sanitize_field(primary_organizational_unit), sanitize_field(primary_institution),
                    sanitize_field(person_identifier), relationshipIdentityCount
                ])
            except Exception as e:
                log_error(person_identifier, f"Error processing identity: {e}")
                continue
        write_csv_rows(output_file, rows)

        # Normalize line endings to Unix-style (\n)
        normalize_line_endings(output_file)

        print(f"Processed {len(rows)} identities successfully.")
    except Exception as e:
        log_error('N/A', f"Error writing to person_temp.csv: {e}")


def normalize_line_endings(file_path):
    """
    Normalize line endings in a file to Unix-style (\n).
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Replace Windows-style line endings with Unix-style
        content = content.replace('\r\n', '\n')

        with open(file_path, 'w', encoding='utf-8', newline='\n') as f:
            f.write(content)
        print(f"Normalized line endings in {file_path}.")
    except Exception as e:
        log_error('N/A', f"Error normalizing line endings in {file_path}: {e}")
        
def process_person(items, output_path):
    """
    Process person data and write to person2.csv.

    Args:
        items (list): List of person data items.
        output_path (str): Path to the output directory.
    """
    output_file = os.path.join(output_path, 'person2.csv')

    # Define headers dynamically based on expected fields
    headers = [
        "personIdentifier", "dateAdded", "dateUpdated", "precision", "recall",
        "countSuggestedArticles", "countPendingArticles", "overallAccuracy", "mode"
    ]

    try:
        # Write headers to the CSV file
        write_csv_header(output_file, headers)

        # Prepare rows for writing
        rows = []
        for item in items:
            try:
                person_identifier = sanitize_field(item.get('personIdentifier', ''))
                if not person_identifier:
                    raise ValueError("Missing personIdentifier")

                date_added = convert_timestamp(item.get('dateAdded'))
                date_updated = convert_timestamp(item.get('dateUpdated'))
                precision = sanitize_field(item.get('precision', ''))
                recall = sanitize_field(item.get('recall', ''))
                count_suggested_articles = sanitize_field(item.get('countSuggestedArticles', ''))
                count_pending_articles = sanitize_field(item.get('countPendingArticles', 0))
                overall_accuracy = sanitize_field(item.get('overallAccuracy', ''))
                mode = sanitize_field(item.get('mode', ''))

                # Add a row to the list
                rows.append([
                    person_identifier, date_added, date_updated, precision, recall,
                    count_suggested_articles, count_pending_articles, overall_accuracy, mode
                ])
            except Exception as e:
                log_error(person_identifier, f"Error processing person: {e}")
                continue

        # Write rows to the CSV file
        write_csv_rows(output_file, rows)
        print(f"Processed {len(rows)} persons successfully.")

    except Exception as e:
        log_error('N/A', f"Error writing to person2.csv: {e}")


def process_person_article(items, output_path):
    """
    Process person articles and write to person_article2.csv.

    Args:
        items (list): List of person data items.
        output_path (str): Path to the output directory.
    """
    output_file = os.path.join(output_path, 'person_article2.csv')

    # Define headers dynamically
    headers = [
        "personIdentifier", "pmid", "authorshipLikelihoodScore", "pmcid",

        "userAssertion", "publicationDateDisplay", "publicationDateStandardized",
        "publicationTypeCanonical", "scopusDocID", "journalTitleVerbose", "articleTitle",
        "articleAuthorNameFirstName", "articleAuthorNameLastName",
        "institutionalAuthorNameFirstName", "institutionalAuthorNameMiddleName",
        "institutionalAuthorNameLastName", "nameMatchFirstScore", "nameMatchFirstType",
        "nameMatchMiddleScore", "nameMatchMiddleType", "nameMatchLastScore",
        "nameMatchLastType", "nameMatchModifierScore", "nameScoreTotal", "emailMatch",
        "emailMatchScore", "journalSubfieldScienceMetrixLabel",
        "journalSubfieldScienceMetrixID", "journalSubfieldDepartment",
        "journalSubfieldScore", "relationshipEvidenceTotalScore",
        "relationshipMinimumTotalScore", "relationshipNonMatchCount",
        "relationshipNonMatchScore", "articleYear",
        
        "identityBachelorYear", "discrepancyDegreeYearBachelor", "discrepancyDegreeYearBachelorScore",
        "identityDoctoralYear", "discrepancyDegreeYearDoctoral", "discrepancyDegreeYearDoctoralScore",
        "genderScoreArticle", "genderScoreIdentity", "genderScoreIdentityArticleDiscrepancy",
        "personType", "personTypeScore", "countArticlesRetrieved", "articleCountScore",
        "targetAuthorInstitutionalAffiliationArticlePubmedLabel",
        "pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore",
        "scopusNonTargetAuthorInstitutionalAffiliationSource",
        "scopusNonTargetAuthorInstitutionalAffiliationScore",
        
        "datePublicationAddedToEntrez", "doi",
        "issn", "issue", "journalTitleISOabbreviation", "pages", "timesCited", "volume",
        
        "feedbackScoreCites", "feedbackScoreCoAuthorName", "feedbackScoreEmail",
        "feedbackScoreInstitution", "feedbackScoreJournal", "feedbackScoreJournalSubField",
        "feedbackScoreKeyword", "feedbackScoreOrcid", "feedbackScoreOrcidCoAuthor",
        "feedbackScoreOrganization", "feedbackScoreTargetAuthorName", "feedbackScoreYear",
        "totalArticleScoreStandardized", "totalArticleScoreNonStandardized"
    ]

    try:
        # Write headers to the CSV file
        write_csv_header(output_file, headers)

        # Prepare rows for writing
        rows = []
        processed_articles = 0
        for item in items:
            person_identifier = sanitize_field(item.get('personIdentifier', ''))
            try:
                articles = item.get('reCiterArticleFeatures', [])
                for article in articles:
                    try:
                        # Process article fields
                        pmid = sanitize_field(article.get('pmid'))
                        if not pmid:
                            continue  # Skip articles without PMID

                        # Top-level article fields
                        authorship_likelihood_score = sanitize_field(article.get('authorshipLikelihoodScore', ''))
                        pmcid = sanitize_field(article.get('pmcid', ''))
                        user_assertion = sanitize_field(article.get('userAssertion', ''))
                        publication_date_display = sanitize_field(article.get('publicationDateDisplay', ''))
                        publication_date_standardized = sanitize_field(article.get('publicationDateStandardized', ''))
                        publication_type_canonical = sanitize_field(article.get('publicationType', {}).get('publicationTypeCanonical', ''))
                        scopus_doc_id = sanitize_field(article.get('scopusDocID', ''))
                        journal_title_verbose = sanitize_field(article.get('journalTitleVerbose', ''))
                        article_title = sanitize_field(article.get('articleTitle', ''))

                        # Evidence fields
                        evidence = article.get('evidence', {})

                        # Author name evidence
                        author_name_evidence = evidence.get('authorNameEvidence', {})
                        article_author_name = author_name_evidence.get('articleAuthorName', {})
                        institutional_author_name = author_name_evidence.get('institutionalAuthorName', {})
                        article_author_first_name = sanitize_field(article_author_name.get('firstName', ''))
                        article_author_last_name = sanitize_field(article_author_name.get('lastName', ''))
                        institutional_author_first_name = sanitize_field(institutional_author_name.get('firstName', ''))
                        institutional_author_middle_name = sanitize_field(institutional_author_name.get('middleName', ''))
                        institutional_author_last_name = sanitize_field(institutional_author_name.get('lastName', ''))
                        name_match_first_score = sanitize_field(author_name_evidence.get('nameMatchFirstScore', ''))
                        name_match_first_type = sanitize_field(author_name_evidence.get('nameMatchFirstType', ''))
                        name_match_middle_score = sanitize_field(author_name_evidence.get('nameMatchMiddleScore', ''))
                        name_match_middle_type = sanitize_field(author_name_evidence.get('nameMatchMiddleType', ''))
                        name_match_last_score = sanitize_field(author_name_evidence.get('nameMatchLastScore', ''))
                        name_match_last_type = sanitize_field(author_name_evidence.get('nameMatchLastType', ''))
                        name_match_modifier_score = sanitize_field(author_name_evidence.get('nameMatchModifierScore', ''))
                        name_score_total = sanitize_field(author_name_evidence.get('nameScoreTotal', ''))

                        # Email evidence
                        email_evidence = evidence.get('emailEvidence', {})
                        email_match = sanitize_field(email_evidence.get('emailMatch', ''))
                        email_match_score = sanitize_field(email_evidence.get('emailMatchScore', ''))

                        # Journal subfield evidence
                        journal_subfield_evidence = evidence.get('journalCategoryEvidence', {})
                        journal_subfield_label = sanitize_field(journal_subfield_evidence.get('journalSubfieldScienceMetrixLabel', ''))
                        journal_subfield_id = sanitize_field(journal_subfield_evidence.get('journalSubfieldScienceMetrixID', ''))
                        journal_subfield_department = sanitize_field(journal_subfield_evidence.get('journalSubfieldDepartment', ''))
                        journal_subfield_score = sanitize_field(journal_subfield_evidence.get('journalSubfieldScore', ''))

                        # Relationship evidence
                        relationship_evidence = evidence.get('relationshipEvidence', {})
                        relationship_total_score = sanitize_field(relationship_evidence.get('relationshipEvidenceTotalScore', ''))

                        # Relationship negative match
                        relationship_negative_match = relationship_evidence.get('relationshipNegativeMatch', {})
                        if isinstance(relationship_negative_match, dict):
                            relationship_min_score = sanitize_field(relationship_negative_match.get('relationshipMinimumTotalScore', ''))
                            relationship_non_match_count = sanitize_field(relationship_negative_match.get('relationshipNonMatchCount', ''))
                            relationship_non_match_score = sanitize_field(relationship_negative_match.get('relationshipNonMatchScore', ''))
                        else:
                            relationship_min_score = ''
                            relationship_non_match_count = ''
                            relationship_non_match_score = ''

                        # Education year evidence
                        education_year_evidence = evidence.get('educationYearEvidence', {})
                        article_year = sanitize_field(education_year_evidence.get('articleYear', ''))
                        identity_bachelor_year = sanitize_field(education_year_evidence.get('identityBachelorYear', ''))
                        discrepancy_degree_year_bachelor = sanitize_field(education_year_evidence.get('discrepancyDegreeYearBachelor', ''))
                        discrepancy_degree_year_bachelor_score = sanitize_field(education_year_evidence.get('discrepancyDegreeYearBachelorScore', ''))
                        identity_doctoral_year = sanitize_field(education_year_evidence.get('identityDoctoralYear', ''))
                        discrepancy_degree_year_doctoral = sanitize_field(education_year_evidence.get('discrepancyDegreeYearDoctoral', ''))
                        discrepancy_degree_year_doctoral_score = sanitize_field(education_year_evidence.get('discrepancyDegreeYearDoctoralScore', ''))

                        # Gender evidence
                        gender_evidence = evidence.get('genderEvidence', {})
                        gender_score_article = sanitize_field(gender_evidence.get('genderScoreArticle', ''))
                        gender_score_identity = sanitize_field(gender_evidence.get('genderScoreIdentity', ''))
                        gender_score_identity_article_discrepancy = sanitize_field(gender_evidence.get('genderScoreIdentityArticleDiscrepancy', ''))

                        # Person type evidence
                        person_type_evidence = evidence.get('personTypeEvidence', {})
                        person_type = sanitize_field(person_type_evidence.get('personType', ''))
                        person_type_score = sanitize_field(person_type_evidence.get('personTypeScore', ''))

                        # Article count evidence
                        article_count_evidence = evidence.get('articleCountEvidence', {})
                        count_articles_retrieved = sanitize_field(article_count_evidence.get('countArticlesRetrieved', ''))
                        # print(f"Debug: Raw field value: {count_articles_retrieved}")
                        article_count_score = sanitize_field(article_count_evidence.get('articleCountScore', ''))
                        # print(f"Debug: Raw field value: {article_count_score}")

                        # PubMed affiliation evidence
                        affiliation_evidence = evidence.get('affiliationEvidence', {})
                        
                        pubmed_target_author_affiliation = affiliation_evidence.get('pubmedTargetAuthorAffiliation', {})
                        # print(f"Debug: Raw field value: {pubmed_target_author_affiliation}")
                        if isinstance(pubmed_target_author_affiliation, dict):
                            target_author_institutional_affiliation_article_pubmed_label = sanitize_field(pubmed_target_author_affiliation.get('targetAuthorInstitutionalAffiliationArticlePubmedLabel', ''))
                            pubmed_target_author_institutional_affiliation_match_type_score = sanitize_field(pubmed_target_author_affiliation.get('targetAuthorInstitutionalAffiliationMatchTypeScore', ''))
                        else:
                            target_author_institutional_affiliation_article_pubmed_label = ''
                            pubmed_target_author_institutional_affiliation_match_type_score = ''

                        # Scopus affiliation evidence
                        scopus_affiliation = affiliation_evidence.get('scopusNonTargetAuthorAffiliation', {})
                        if isinstance(scopus_affiliation, dict):
                            # Handle dict case directly
                            scopus_non_target_author_institutional_affiliation_source = sanitize_field(scopus_affiliation.get('nonTargetAuthorInstitutionalAffiliationSource', ''))
                            scopus_non_target_author_institutional_affiliation_score = sanitize_field(scopus_affiliation.get('nonTargetAuthorInstitutionalAffiliationScore', 0))
                        elif isinstance(scopus_affiliation, list) and scopus_affiliation:
                            # Handle list case by taking the first element
                            first_affiliation = scopus_affiliation[0]
                            if isinstance(first_affiliation, dict):
                                scopus_non_target_author_institutional_affiliation_source = sanitize_field(first_affiliation.get('nonTargetAuthorInstitutionalAffiliationSource', ''))
                                scopus_non_target_author_institutional_affiliation_score = sanitize_field(first_affiliation.get('nonTargetAuthorInstitutionalAffiliationScore', 0))

                        # Additional fields
                        date_publication_added_to_entrez = sanitize_field(article.get('datePublicationAddedToEntrez', ''))
                        doi = sanitize_field(article.get('doi', ''))
                        issn_list = article.get('issn', [])
                        if isinstance(issn_list, list):
                            for issn_item in issn_list:
                                issntype = issn_item.get('issntype', '')
                                # Prioritize 'Linking', then 'Print', then 'Electronic'
                                if issntype == 'Linking':
                                    issn = sanitize_field(issn_item.get('issn', ''))
                                    break
                                if issntype == 'Print':
                                    issn = sanitize_field(issn_item.get('issn', ''))
                                    break
                                if issntype == 'Electronic':
                                    issn = sanitize_field(issn_item.get('issn', ''))
                                    break
                        else:
                            # Handle case where issn is not a list (fallback)
                            issn = sanitize_field(issn_list)
                            
                        issue = sanitize_field(article.get('issue', ''))
                        journal_title_iso = sanitize_field(article.get('journalTitleISOabbreviation', ''))
                        pages = sanitize_field(article.get('pages', ''))
                        times_cited = sanitize_field(article.get('timesCited', ''))
                        volume = sanitize_field(article.get('volume', ''))

                        # Feedback scores
                        feedback_evidence = evidence.get('feedbackEvidence', {})
                        if isinstance(feedback_evidence, dict):
                            feedback_scores = [
                                sanitize_field(feedback_evidence.get(key, '')) for key in [
                                    'feedbackScoreCites', 'feedbackScoreCoAuthorName', 'feedbackScoreEmail',
                                    'feedbackScoreInstitution', 'feedbackScoreJournal', 'feedbackScoreJournalSubField',
                                    'feedbackScoreKeyword', 'feedbackScoreOrcid', 'feedbackScoreOrcidCoAuthor',
                                    'feedbackScoreOrganization', 'feedbackScoreTargetAuthorName', 'feedbackScoreYear'
                                ]
                            ]
                        else:
                            feedback_scores = [''] * 12  # Assuming 12 feedback scores
                        total_article_score_standardized = sanitize_field(article.get('totalArticleScoreStandardized', ''))
                        total_article_score_non_standardized = sanitize_field(article.get('totalArticleScoreNonStandardized', ''))                            

                        # Construct a dictionary to map headers to values
                        row_data = {
                            "personIdentifier": person_identifier,
                            "pmid": pmid,
                            "authorshipLikelihoodScore": authorship_likelihood_score,
                            "pmcid": pmcid,
                            "userAssertion": user_assertion,
                            "publicationDateDisplay": publication_date_display,
                            "publicationDateStandardized": publication_date_standardized,
                            "publicationTypeCanonical": publication_type_canonical,
                            "scopusDocID": scopus_doc_id,
                            "journalTitleVerbose": journal_title_verbose,
                            "articleTitle": article_title,
                            "articleAuthorNameFirstName": article_author_first_name,
                            "articleAuthorNameLastName": article_author_last_name,
                            "institutionalAuthorNameFirstName": institutional_author_first_name,
                            "institutionalAuthorNameMiddleName": institutional_author_middle_name,
                            "institutionalAuthorNameLastName": institutional_author_last_name,
                            "nameMatchFirstScore": name_match_first_score,
                            "nameMatchFirstType": name_match_first_type,
                            "nameMatchMiddleScore": name_match_middle_score,
                            "nameMatchMiddleType": name_match_middle_type,
                            "nameMatchLastScore": name_match_last_score,
                            "nameMatchLastType": name_match_last_type,
                            "nameMatchModifierScore": name_match_modifier_score,
                            "nameScoreTotal": name_score_total,
                            "emailMatch": email_match,
                            "emailMatchScore": email_match_score,
                            "journalSubfieldScienceMetrixLabel": journal_subfield_label,
                            "journalSubfieldScienceMetrixID": journal_subfield_id,
                            "journalSubfieldDepartment": journal_subfield_department,
                            "journalSubfieldScore": journal_subfield_score,
                            "relationshipEvidenceTotalScore": relationship_total_score,
                            "relationshipMinimumTotalScore": relationship_min_score,
                            "relationshipNonMatchCount": relationship_non_match_count,
                            "relationshipNonMatchScore": relationship_non_match_score,
                            "articleYear": article_year,

                            "identityBachelorYear": identity_bachelor_year,
                            "discrepancyDegreeYearBachelor": discrepancy_degree_year_bachelor,
                            "discrepancyDegreeYearBachelorScore": discrepancy_degree_year_bachelor_score,
                            "identityDoctoralYear": identity_doctoral_year,
                            "discrepancyDegreeYearDoctoral": discrepancy_degree_year_doctoral,
                            "discrepancyDegreeYearDoctoralScore": discrepancy_degree_year_doctoral_score,
                            "genderScoreArticle": gender_score_article,
                            "genderScoreIdentity": gender_score_identity,
                            "genderScoreIdentityArticleDiscrepancy": gender_score_identity_article_discrepancy,
                            "personType": person_type,
                            "personTypeScore": person_type_score,
                            "countArticlesRetrieved": count_articles_retrieved,
                            "articleCountScore": article_count_score,
                            "targetAuthorInstitutionalAffiliationArticlePubmedLabel": target_author_institutional_affiliation_article_pubmed_label,
                            "pubmedTargetAuthorInstitutionalAffiliationMatchTypeScore": pubmed_target_author_institutional_affiliation_match_type_score,
                            "scopusNonTargetAuthorInstitutionalAffiliationSource": scopus_non_target_author_institutional_affiliation_source,
                            "scopusNonTargetAuthorInstitutionalAffiliationScore": scopus_non_target_author_institutional_affiliation_score,
                                                       
                            "datePublicationAddedToEntrez": date_publication_added_to_entrez,
                            "doi": doi,
                            "issn": issn,
                            "issue": issue,
                            "journalTitleISOabbreviation": journal_title_iso,
                            "pages": pages,
                            "timesCited": times_cited,
                            "volume": volume,

                            # Include feedback scores
                            "feedbackScoreCites": feedback_scores[0],
                            "feedbackScoreCoAuthorName": feedback_scores[1],
                            "feedbackScoreEmail": feedback_scores[2],
                            "feedbackScoreInstitution": feedback_scores[3],
                            "feedbackScoreJournal": feedback_scores[4],
                            "feedbackScoreJournalSubField": feedback_scores[5],
                            "feedbackScoreKeyword": feedback_scores[6],
                            "feedbackScoreOrcid": feedback_scores[7],
                            "feedbackScoreOrcidCoAuthor": feedback_scores[8],
                            "feedbackScoreOrganization": feedback_scores[9],
                            "feedbackScoreTargetAuthorName": feedback_scores[10],
                            "feedbackScoreYear": feedback_scores[11],
                            
                            "totalArticleScoreStandardized": total_article_score_standardized,
                            "totalArticleScoreNonStandardized": total_article_score_non_standardized
                        }

                        # Build the row using the headers to ensure correct order
                        row = [row_data.get(header, '') for header in headers]

                        # Debugging statements
                        # print(f"Debug: Length of headers: {len(headers)}")
                        # print(f"Debug: Length of row: {len(row)}")
                        # print(f"Debug: Row data: {row}")

                        rows.append(row)
                        processed_articles += 1
                    except Exception as e:
                        log_error(person_identifier, f"Error processing article PMID {pmid}: {e}")
                        continue
            except Exception as e:
                log_error(person_identifier, f"Error processing articles: {e}")
                continue

        # Write rows to the CSV file
        write_csv_rows(output_file, rows)
        print(f"Processed {processed_articles} articles successfully.")

    except Exception as e:
        log_error('N/A', f"Error writing to person_article2.csv: {e}")


def process_person_article_author(items, output_path):
    """
    Process person article author data and write to person_article_author2.csv.

    Args:
        items (list): List of person data items.
        output_path (str): Path to the output directory.
    """
    output_file = os.path.join(output_path, 'person_article_author2.csv')

    # Define headers
    headers = ["personIdentifier", "pmid", "firstName", "lastName", "equalContrib", "rank", "orcid", "targetAuthor"]

    try:
        # Write headers to the CSV file
        write_csv_header(output_file, headers)

        # Prepare rows for writing
        rows = []
        no_author_features_list = []

        for item in items:
            person_identifier = sanitize_field(item.get('personIdentifier', ''))
            try:
                articles = item.get('reCiterArticleFeatures', [])
                for article in articles:
                    pmid = sanitize_field(article.get('pmid', 0))  # Ensure pmid is sanitized

                    # Extract author features
                    author_features = article.get('reCiterArticleAuthorFeatures', [])
                    if author_features:
                        for author in author_features:
                            # Extract and sanitize author details
                            first_name = sanitize_field(author.get('firstName', ''))
                            last_name = sanitize_field(author.get('lastName', ''))
                            equal_contrib = sanitize_field(author.get('equalContrib', ''))
                            rank = sanitize_field(str(author.get('rank', 0)))  # Ensure rank is sanitized as a string
                            orcid = sanitize_field(author.get('orcid', ''))
                            target_author = "1" if author.get('targetAuthor', False) else "0"

                            # Append the row
                            rows.append([
                                person_identifier, pmid, first_name, last_name, equal_contrib, rank, orcid, target_author
                            ])
                    else:
                        # Track articles with no author features
                        no_author_features_list.append((person_identifier, pmid))
            except Exception as e:
                log_error(person_identifier, f"Error processing article authors: {e}")
                continue

        # Write rows to the CSV file
        write_csv_rows(output_file, rows)

        # Log the results
        print(f"Processed {len(rows)} author records successfully.")
        if no_author_features_list:
            print(f"No author features for {len(no_author_features_list)} articles: {no_author_features_list}")

    except Exception as e:
        log_error('N/A', f"Error writing to person_article_author2.csv: {e}")

def process_person_article_department(items, output_path):
    """
    Process department affiliations and write to person_article_department2.csv.

    Args:
        items (list): List of person items containing department evidence.
        output_path (str): Path to the directory where the CSV will be written.
    """
    file_path = os.path.join(output_path, 'person_article_department2.csv')

    # Define the headers
    headers = [
        "personIdentifier", "pmid", "identityOrganizationalUnit", "articleAffiliation",
        "organizationalUnitType", "organizationalUnitMatchingScore",
        "organizationalUnitModifier", "organizationalUnitModifierScore"
    ]

    try:
        # Write the headers
        write_csv_header(file_path, headers)

        rows = []
        for item in items:
            person_identifier = sanitize_field(item.get('personIdentifier', ''))
            try:
                # Iterate through article features
                for article in item.get('reCiterArticleFeatures', []):
                    pmid = article.get('pmid', 0)

                    # Extract organizational unit evidence
                    org_units = article.get('evidence', {}).get('organizationalUnitEvidence', [])
                    for org_unit in org_units:
                        rows.append([
                            person_identifier,
                            pmid,
                            sanitize_field(org_unit.get('identityOrganizationalUnit', '')),
                            sanitize_field(org_unit.get('articleAffiliation', '')),
                            sanitize_field(org_unit.get('organizationalUnitType', '')),
                            sanitize_field(org_unit.get('organizationalUnitMatchingScore', '')),
                            sanitize_field(org_unit.get('organizationalUnitModifier', '')),
                            sanitize_field(org_unit.get('organizationalUnitModifierScore', ''))
                        ])
            except Exception as e:
                log_error(person_identifier, f"Error processing department affiliations: {e}")
                continue

        # Write all rows to the CSV
        write_csv_rows(file_path, rows)

        print(f"Processed {len(rows)} rows for person_article_department.")

    except Exception as e:
        log_error('N/A', f"Error writing to person_article_department2.csv: {e}")

def process_person_article_grant(items, output_path):
    """
    Process person article grant data and write to person_article_grant2.csv.

    Args:
        items (list): List of person data items containing grant evidence.
        output_path (str): Path to the directory where the CSV will be written.
    """
    file_path = os.path.join(output_path, 'person_article_grant2.csv')

    # Define headers
    headers = ["personIdentifier", "pmid", "articleGrant", "grantMatchScore", "institutionGrant"]

    try:
        # Write headers to the file
        write_csv_header(file_path, headers)

        rows = []
        for item in items:
            person_identifier = sanitize_field(item.get('personIdentifier', ''))
            try:
                for article in item.get('reCiterArticleFeatures', []):
                    pmid = article.get('pmid', 0)  # PMIDs are typically integers

                    # Extract grant evidence
                    grant_evidence = article.get('evidence', {}).get('grantEvidence', {})
                    grants = grant_evidence.get('grants', [])

                    for grant in grants:
                        article_grant = sanitize_field(grant.get('articleGrant', ''))
                        grant_match_score = sanitize_field(grant.get('grantMatchScore', ''))
                        institution_grant = sanitize_field(grant.get('institutionGrant', ''))

                        # Append the row
                        rows.append([
                            person_identifier, pmid, article_grant, grant_match_score, institution_grant
                        ])
            except Exception as e:
                log_error(person_identifier, f"Error processing grants: {e}")
                continue

        # Write all rows to the CSV
        write_csv_rows(file_path, rows)

        print(f"Processed {len(rows)} rows for person_article_grant.")

    except Exception as e:
        log_error('N/A', f"Error writing to person_article_grant2.csv: {e}")

def process_person_article_keyword(items, output_path):
    """
    Process article keywords and write to person_article_keyword2.csv.

    Args:
        items (list): List of person data items containing article keywords.
        output_path (str): Path to the directory where the CSV will be written.
    """
    file_path = os.path.join(output_path, 'person_article_keyword2.csv')

    # Define headers
    headers = ["personIdentifier", "keyword", "pmid"]

    try:
        # Write headers to the file
        write_csv_header(file_path, headers)

        rows = []
        for item in items:
            person_identifier = sanitize_field(item.get('personIdentifier', ''))
            try:
                for article in item.get('reCiterArticleFeatures', []):
                    pmid = article.get('pmid', 0)  # PMIDs are typically integers

                    # Extract keywords if present
                    keywords = article.get('articleKeywords', [])
                    for keyword_entry in keywords:
                        keyword = sanitize_field(keyword_entry.get('keyword', ''))

                        # Validate row data
                        if not person_identifier or not pmid or not keyword:
                            log_error(person_identifier, f"Invalid row data: {person_identifier}, {pmid}, {keyword}")
                            continue

                        # Append the row
                        rows.append([
                            person_identifier,
                            keyword,
                            pmid
                        ])
            except Exception as e:
                log_error(person_identifier, f"Error processing keywords: {e}")
                continue

        # Write all rows to the CSV
        write_csv_rows(file_path, rows)

        print(f"Processed {len(rows)} rows for person_article_keyword.")

    except Exception as e:
        log_error('N/A', f"Error writing to person_article_keyword2.csv: {e}")
        
def process_person_article_relationship(items, output_path):
    """
    Process relationship evidence and write to person_article_relationship2.csv.

    Args:
        items (list): List of person items containing relationship evidence.
        output_path (str): Path to the directory where the CSV will be written.
    """
    file_path = os.path.join(output_path, 'person_article_relationship2.csv')

    # Define the headers
    headers = [
        "personIdentifier", "pmid", "relationshipNameArticleFirstName",
        "relationshipNameArticleLastName", "relationshipNameIdentityFirstName",
        "relationshipNameIdentityLastName", "relationshipType", "relationshipMatchType",
        "relationshipMatchingScore", "relationshipVerboseMatchModifierScore",
        "relationshipMatchModifierMentor", "relationshipMatchModifierMentorSeniorAuthor",
        "relationshipMatchModifierManager", "relationshipMatchModifierManagerSeniorAuthor"
    ]

    try:
        # Write the headers
        write_csv_header(file_path, headers)

        rows = []
        for item in items:
            person_identifier = sanitize_field(item.get('personIdentifier', ''))
            try:
                for article in item.get('reCiterArticleFeatures', []):
                    pmid = article.get('pmid', 0)
                    relationship_evidence = article.get('evidence', {}).get('relationshipEvidence', {})

                    if relationship_evidence:
                        # Process 'relationshipPositiveMatch' if it exists
                        positive_matches = relationship_evidence.get('relationshipPositiveMatch', [])
                        for relation in positive_matches:
                            try:
                                # Handle potential misspelling of keys
                                identity_name = relation.get('relationshipNameIdentity', relation.get('relationshipNameIdenity', {}))
                                identity_first_name = sanitize_field(identity_name.get('firstName', ''))
                                identity_last_name = sanitize_field(identity_name.get('lastName', ''))

                                row = [
                                    person_identifier,
                                    pmid,
                                    sanitize_field(relation.get('relationshipNameArticle', {}).get('firstName', '')),
                                    sanitize_field(relation.get('relationshipNameArticle', {}).get('lastName', '')),
                                    identity_first_name,
                                    identity_last_name,
                                    sanitize_field(relation.get('relationshipType', '')),
                                    sanitize_field(relation.get('relationshipMatchType', '')),
                                    sanitize_field(relation.get('relationshipMatchingScore', '')),
                                    sanitize_field(relation.get('relationshipVerboseMatchModifierScore', '')),
                                    sanitize_field(relation.get('relationshipMatchModifierMentor', '')),
                                    sanitize_field(relation.get('relationshipMatchModifierMentorSeniorAuthor', '')),
                                    sanitize_field(relation.get('relationshipMatchModifierManager', '')),
                                    sanitize_field(relation.get('relationshipMatchModifierManagerSeniorAuthor', ''))
                                ]
                                rows.append(row)
                            except Exception as e:
                                log_error(person_identifier, f"Error processing relation in article {pmid}: {e}")
                                continue
                    else:
                        # If 'relationshipEvidence' is empty or missing
                        continue
            except Exception as e:
                log_error(person_identifier, f"Error processing relationship evidence for article {pmid}: {e}")
                continue

        # Write rows to CSV
        write_csv_rows(file_path, rows)
        print(f"Processed {len(rows)} rows for article relationships.")

    except Exception as e:
        log_error('N/A', f"Error writing to person_article_relationship2.csv: {e}")


def process_person_article_scopus_non_target_author_affiliation(items, output_path):
    """
    Process Scopus non-target author affiliations and write to a CSV file.

    Args:
        items (list): List of data items containing Scopus non-target affiliation information.
        output_path (str): Path to the directory where the CSV will be written.
    """
    file_path = os.path.join(output_path, 'person_article_scopus_non_target_author_affiliation2.csv')

    # Define headers
    headers = [
        "personIdentifier", "pmid", "nonTargetAuthorInstitutionLabel",
        "nonTargetAuthorInstitutionID", "nonTargetAuthorInstitutionCount"
    ]

    try:
        # Open the file and write the header row
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(headers)

            count = 0
            for item in items:
                try:
                    person_identifier = sanitize_field(item.get('personIdentifier', 'NULL'))
                    article_features = item.get('reCiterArticleFeatures', [])

                    for article in article_features:
                        pmid = sanitize_field(article.get('pmid', 0))  # PMIDs are typically integers

                        # Extract affiliation evidence
                        affiliation_evidence = article.get('evidence', {}).get('affiliationEvidence', {})
                        scopus_non_target_affiliations = affiliation_evidence.get(
                            'scopusNonTargetAuthorAffiliation', {}
                        )

                        # Handle known institution matches
                        known_institutions = scopus_non_target_affiliations.get(
                            'nonTargetAuthorInstitutionalAffiliationMatchKnownInstitution', []
                        )

                        # Normalize known_institutions to ensure it's always a list
                        if isinstance(known_institutions, str):
                            # Single string, convert to a list with one item
                            known_institutions = [known_institutions]
                        elif isinstance(known_institutions, dict):
                            # Single dictionary, convert to a list with one item
                            known_institutions = [known_institutions]
                        elif not isinstance(known_institutions, list):
                            # Unexpected type, log and skip
                            log_error(
                                person_identifier,
                                f"Unexpected type for known_institutions in article {pmid}: {type(known_institutions)}"
                            )
                            continue

                        # Process each affiliation
                        for affiliation in known_institutions:
                            if isinstance(affiliation, str):
                                # Handle embedded commas
                                count_comma = affiliation.count(',')
                                if count_comma > 2:
                                    # Replace extra commas in the institution label
                                    extra_comma_count = count_comma - 2
                                    affiliation = affiliation.replace(",", ".", extra_comma_count)

                                # Parse the affiliation string
                                components = affiliation.split(',')
                                if len(components) == 3:
                                    writer.writerow([
                                        person_identifier,
                                        pmid,
                                        sanitize_field(components[0]),  # Institution label
                                        sanitize_field(components[1]),  # Institution ID
                                        sanitize_field(components[2])   # Institution count
                                    ])
                                else:
                                    log_error(
                                        person_identifier,
                                        f"Unexpected string format for affiliation in article {pmid}: {affiliation}"
                                    )
                            else:
                                log_error(person_identifier, f"Non-string affiliation found in article {pmid}: {affiliation}")
                except Exception as e:
                    log_error(person_identifier, f"Error processing Scopus non-target affiliations: {e}")

                count += 1
                # print("Processed item:", count)

        print(f"Processed all items for Scopus non-target author affiliations.")

    except Exception as e:
        log_error('N/A', f"Error writing to person_article_scopus_non_target_author_affiliation2.csv: {e}")
        
def process_person_article_scopus_target_author_affiliation(items, output_path):
    """
    Process Scopus target author affiliations and write to a CSV file.

    Args:
        items (list): List of data items containing Scopus target affiliation information.
        output_path (str): Path to the directory where the CSV will be written.
    """
    file_path = os.path.join(output_path, 'person_article_scopus_target_author_affiliation2.csv')

    # Define headers
    headers = [
        "personIdentifier", "pmid", "targetAuthorInstitutionalAffiliationSource",
        "scopusTargetAuthorInstitutionalAffiliationIdentity",
        "targetAuthorInstitutionalAffiliationArticleScopusLabel",
        "targetAuthorInstitutionalAffiliationArticleScopusAffiliationId",
        "targetAuthorInstitutionalAffiliationMatchType",
        "targetAuthorInstitutionalAffiliationMatchTypeScore"
    ]

    try:
        # Write headers to the file
        write_csv_header(file_path, headers)

        rows = []
        for item in items:
            person_identifier = sanitize_field(item.get('personIdentifier', ''))
            
            for article in item.get('reCiterArticleFeatures', []):
                pmid = article.get('pmid', 0)  # PMIDs are typically integers

                # Extract Scopus target affiliation evidence
                scopus_target_affiliations = (
                    article.get('evidence', {})
                          .get('affiliationEvidence', {})
                          .get('scopusTargetAuthorAffiliation', [])
                )

                for affiliation in scopus_target_affiliations:
                    try:
                        # Skip records where the affiliation ID is missing or invalid
                        affiliation_id = affiliation.get('targetAuthorInstitutionalAffiliationArticleScopusAffiliationId')
                        if not affiliation_id or affiliation_id == 0:
                            continue

                        rows.append([
                            person_identifier,
                            pmid,
                            sanitize_field(affiliation.get('targetAuthorInstitutionalAffiliationSource', '')),
                            sanitize_field(affiliation.get('targetAuthorInstitutionalAffiliationIdentity', '')),
                            sanitize_field(affiliation.get('targetAuthorInstitutionalAffiliationArticleScopusLabel', '')),
                            affiliation_id,  # Validated as non-zero
                            sanitize_field(affiliation.get('targetAuthorInstitutionalAffiliationMatchType', '')),
                            sanitize_field(affiliation.get('targetAuthorInstitutionalAffiliationMatchTypeScore', ''))
                        ])
                    except Exception as e:
                        log_error(person_identifier, f"Error processing target affiliation in article {pmid}: {e}")
                        continue

        # Write rows to the file
        if rows:
            write_csv_rows(file_path, rows)
            print(f"Processed {len(rows)} rows for Scopus target author affiliations.")
        else:
            print(f"No rows to write for Scopus target author affiliations.")

    except Exception as e:
        log_error('N/A', f"Error writing to person_article_scopus_target_author_affiliation2.csv: {e}")


def process_person_person_type(identities, output_path):
    """
    Process person types and write to person_person_type.csv.

    Args:
        identities (list): List of identity items containing person types.
        output_path (str): Path to the directory where the CSV will be written.
    """
    file_path = os.path.join(output_path, 'person_person_type.csv')

    # Define headers
    headers = ["personIdentifier", "personType"]

    try:
        # Write headers to the file
        write_csv_header(file_path, headers)

        rows = []
        for identity in identities:
            person_identifier = sanitize_field(identity.get('uid', ''))
            try:
                person_types = identity.get('identity', {}).get('personTypes', [])
                for person_type in person_types:
                    rows.append([person_identifier, sanitize_field(person_type)])
            except Exception as e:
                log_error(person_identifier, f"Error processing person types: {e}")
                continue

        # Write rows to the file
        write_csv_rows(file_path, rows)
        
        # Normalize line endings to Unix-style (\n)
        normalize_line_endings(file_path)        

        print(f"Processed {len(rows)} person types.")

    except Exception as e:
        log_error('N/A', f"Error writing to person_person_type.csv: {e}")