# ReCiterDB

- [Summary](#summary)
- [Functionality](#functionality)
- [Architecture](#architecture)
  - [Nightly Pipeline](#nightly-pipeline)
  - [Directory Structure](#directory-structure)
- [Technical](#technical)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
    - [Locally](#locally)
    - [On AWS (EKS)](#on-aws-eks)
  - [Credentials](#credentials)
  - [Environment Variables](#environment-variables)
- [Database Schema](#database-schema)
  - [Key Tables](#key-tables)
  - [Stored Procedures](#stored-procedures)
- [Components](#components)
  - [Setup](#setup)
  - [Update (Nightly Pipeline)](#update-nightly-pipeline)
- [Operations](#operations)
  - [Common Tasks](#common-tasks)
  - [Troubleshooting](#troubleshooting)
- [Configuration](#configuration)
- [More on the ReCiter suite of applications](#more-on-the-reciter-suite-of-applications)



## Summary

ReCiterDB is an open source [MariaDB](https://mariadb.org/) database and set of tools that stores publication lists and computes bibliometric statistics for an academic institution's faculty and other people of interest. ReCiterDB is designed to be populated by person and publication data from [ReCiter](https://github.com/wcmc-its/reciter) (a machine learning-driven publication suggestion engine) and from third party sources such as NIH's iCite and Digital Science's Altmetric services. The data in the system can be viewed using the [ReCiter Publication Manager](https://github.com/wcmc-its/reciter-publication-manager) web application, or it can serve as a stand alone reporting database. For more on the functionality in Publication Manager, see that repository.

This repository contains:

- A MariaDB schema for ReCiterDB
- Stored procedures for computing bibliometric summary tables with zero-downtime atomic table swaps
- Python and shell scripts for importing data from ReCiter, NIH iCite, and Altmetric
- Docker and Kubernetes configs for automated nightly deployment on AWS EKS

<img src="https://github.com/wcmc-its/ReCiterDB/blob/master/files/reCiterReportingModel.png" width=800 />



## Functionality

In conjunction with data from [ReCiter](https://github.com/wcmc-its/reciter), ReCiterDB has been used to answer questions such as the following:

- Senior-authored academic articles in Department of Anesthesiology
- Percentage of full-time faculty publications that were indexed in PubMed with an ORCID identifier
- Publications by full-time faculty added in the past week
- h5 index of full-time faculty
- Which active full-time faculty does any given faculty cite most often on their papers?
- Which faculty publish the most frequently on cancer, overall and by proportion of their total scholarly output?
- What percent of papers published by a given faculty are in collaboration with existing members of the Cancer Center?
- What are the most influential cancer-related papers by members of the Cancer Center?
- Finally, a variety of person-level bibliometric statistics are available through a bibliometric report that can be generated on demand (see [sample](https://github.com/wcmc-its/ReCiterDB/blob/master/files/sampleBibliometricReport.rtf))



## Architecture

### Nightly Pipeline

The system runs as a Kubernetes CronJob. Entry point is `run_all.py`:

```
CronJob (daily)
 └─ run_all.py
     ├─ 1. executeFeatureGenerator.py    Trigger ReCiter ML scoring API
     ├─ 2. retrieveArticles.py           Pull person/article data from S3 + DynamoDB
     ├─ 3. retrieveNIH.py                NIH iCite API → analysis_nih (atomic swap)
     ├─ 4. run_nightly_indexing.sh        Run populateAnalysisSummaryTables_v2()
     │      ├─ Polls analysis_job_log every 3s for progress
     │      ├─ Auto-retries 3x with 60s backoff
     │      └─ Auto-restores from backup on failure
     ├─ 5. abstractImport.py             PubMed abstracts from DynamoDB
     └─ 6. conflictsImport.py            COI statements from DynamoDB
```

**Key patterns:**

- **Atomic table swap**: Data is loaded into staging tables (`_new` suffix), validated against production row counts, then swapped via `RENAME TABLE` (zero downtime). Used by both `retrieveNIH.py` and the main stored procedure.
- **Validation gate**: `retrieveNIH.py` requires staging to have >= 80% of production's unique row count before swapping, preventing bad data from reaching production.
- **Set-based h-index**: Uses `ROW_NUMBER()` window functions instead of loop-based computation.
- **Bulk loading**: `LOAD DATA LOCAL INFILE` for 10-100x faster imports vs. row-by-row inserts.


### Directory Structure

```
ReCiterDB/
├── setup/                                         # Database initialization
│   ├── setupReciterDB.py                          # Orchestrator: runs schema SQL
│   ├── createDatabaseTableReciterDb.sql           # All table DDL
│   ├── createEventsProceduresReciterDb.sql        # All stored procedures
│   ├── insertBaselineDataReciterDb.sql            # Seed data (roles, special chars, journals)
│   ├── populateAnalysisSummaryTables_v2.sql       # Main nightly SP (standalone copy)
│   ├── setup_job_logging.sql                      # Job log table DDL
│   ├── log_progress.sql                           # Progress logging SP
│   ├── cleanup_staging_tables_v2.sql              # Cleanup SP
│   └── restore_from_backup_v2.sql                 # Recovery SP
│
├── update/                                        # Daily ETL pipeline
│   ├── run_all.py                                 # EKS orchestrator (entry point)
│   ├── run_nightly_indexing.sh                    # SP runner with monitoring/retry
│   ├── retrieveArticles.py                        # S3 + DynamoDB article fetcher
│   ├── retrieveNIH.py                             # NIH iCite fetcher (atomic swap)
│   ├── retrieveAltmetric.py                       # Altmetric API fetcher
│   ├── updateReciterDB.py                         # Bulk loader (LOAD DATA LOCAL INFILE)
│   ├── dataTransformer.py                         # ReCiter JSON → CSV
│   ├── abstractImport.py                          # Abstract importer from DynamoDB
│   ├── conflictsImport.py                         # COI statement importer
│   └── executeFeatureGenerator.py                 # Triggers ReCiter scoring API
│
├── kubernetes/                                    # K8s deployment
│   ├── k8-cronjob.yaml                            # CronJob definition
│   ├── k8-configmap.yaml                          # Non-secret config
│   └── k8-secrets.yaml                            # Secret template (placeholders only)
│
├── Dockerfile                                     # Python 3.12-slim container
├── buildspec.yml                                  # CodeBuild → ECR
├── k8-buildspec.yml                               # CodeBuild → EKS image update
└── requirements.txt                               # Python dependencies
```


## Technical

### Prerequisites

- **MariaDB** (not MySQL). The stored procedures use MariaDB-specific features (e.g., `REGEXP_REPLACE` with `BINARY` modifier).
- **Populated instance of ReCiter**. This is where all the person and publication data live.
- **ReCiter Publication Manager (optional)**. Needed for viewing data through a web interface.

### Installation

#### Locally

1. Clone this repository.
2. Install MariaDB and create the database:
```sql
CREATE DATABASE IF NOT EXISTS `reciterdb` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'admin' IDENTIFIED BY '<your password>';
GRANT ALL PRIVILEGES ON *.* TO 'admin';
```

3. Set environment variables (see [Environment Variables](#environment-variables) below).

4. Install dependencies and run setup:
```bash
pip install -r requirements.txt
cd setup && python3 setupReciterDB.py
```

5. Run the daily update:
```bash
python3 update/run_all.py
```

#### On AWS (EKS)

The project is packaged as a Docker container and deployed as a Kubernetes CronJob. See `kubernetes/` for deployment configs and `Dockerfile` for the container definition. For full infrastructure setup, see [ReCiter-CDK](https://github.com/wcmc-its/ReCiter-CDK).


### Credentials

**Credentials must never be committed to this repository.** All secrets are managed through:

- **Kubernetes Secrets** — `kubernetes/k8-secrets.yaml` is a **template** with placeholders. Actual secrets are stored in the EKS cluster.
- **Environment variables** — All scripts read credentials from env vars at runtime.
- **AWS Secrets Manager** — Docker Hub credentials for builds are fetched from Secrets Manager during CodeBuild.

For local development, export env vars in your terminal session or use a MySQL options file (`~/.my.cnf` with `chmod 600`).


### Environment Variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `DB_HOST` | MariaDB hostname | Yes |
| `DB_USERNAME` | Database user | Yes |
| `DB_PASSWORD` | Database password | Yes |
| `DB_NAME` | Database name (e.g., `reciterdb`) | Yes |
| `AWS_ACCESS_KEY_ID` | For S3/DynamoDB access | Yes |
| `AWS_SECRET_ACCESS_KEY` | For S3/DynamoDB access | Yes |
| `AWS_DEFAULT_REGION` | AWS region (e.g., `us-east-1`) | Yes |
| `URL` | ReCiter feature generator endpoint | For feature generation |
| `API_KEY` | ReCiter API auth token | For feature generation |
| `LOG_FILE` | Log file path | No |
| `S3_BUCKET` | S3 bucket for log archival | No |
| `S3_KEY_PREFIX` | S3 key prefix for logs | No |
| `SCRIPT_TIMEOUT_SECONDS` | Pipeline timeout (default: 15000s) | No |



## Database Schema

### Key Tables

**Summary tables** (populated by the nightly stored procedure, read by Publication Manager):

| Table | Purpose |
|-------|---------|
| `analysis_summary_article` | One row per pmid: title, journal, citations, NIH percentile, RCR, impact scores |
| `analysis_summary_author` | One row per person+pmid: author list string, author position (first/last) |
| `analysis_summary_author_list` | One row per author+pmid: individual author name and rank |
| `analysis_summary_person` | One row per person: h-index (NIH + Scopus), article counts, percentile rankings |

**Data source tables** (populated by ETL scripts):

| Table | Purpose |
|-------|---------|
| `person_article` | ReCiter scoring output (~80 columns of evidence per person+pmid pair) |
| `person_article_author` | Author list with rank, targetAuthor flag, ORCID, equalContrib |
| `analysis_nih` | NIH iCite metrics: RCR, percentile, citation count, field citation rate. **UNIQUE KEY on pmid.** |
| `analysis_nih_cites` / `analysis_nih_cites_clin` | Citation networks (multiple rows per pmid) |
| `analysis_altmetric` | Altmetric social metrics |

**Admin tables** (managed by Publication Manager):

| Table | Purpose |
|-------|---------|
| `admin_users`, `admin_roles`, `admin_users_roles` | Role-based access control |
| `admin_feedback_log` | User curation actions |
| `admin_settings` | UI configuration (JSON) |
| `analysis_summary_person_scope` | Defines who gets bibliometric analysis |
| `analysis_override_author_position` | Manual author position corrections |

**Reference data** (loaded once at setup):

| Table | Purpose |
|-------|---------|
| `journal_impact_scimago`, `journal_impact_alternative` | Journal rankings |
| `journal_nlm` | NLM catalog |
| `analysis_special_characters` | 294 Unicode-to-RTF character mappings |

See `setup/createDatabaseTableReciterDb.sql` for the complete schema.


### Stored Procedures

All defined in `setup/createEventsProceduresReciterDb.sql`.

**Nightly indexing:**

| Procedure | Purpose |
|-----------|---------|
| `populateAnalysisSummaryTables_v2()` | Main job: rebuild all `analysis_summary_*` tables with atomic swap |
| `log_progress()` | Write progress entries to `analysis_job_log` |
| `cleanup_staging_tables_v2()` | Drop `*_new` staging tables on failure |
| `restore_from_backup_v2()` | Rename `*_backup` tables back to production |
| `check_analysis_summary_status()` | Show row counts and backup table status |
| `view_job_progress(job_id)` | Show all log entries for a job |

**Report generation:**

| Procedure | Purpose |
|-----------|---------|
| `generateBibliometricReport(personID)` | Full bibliometric report for one person |
| `generatePubsRTF()` | RTF publication list |
| `generateEmailNotifications()` | New publication email alerts |

**Admin:**

| Procedure | Purpose |
|-----------|---------|
| `populateAnalysisSummaryPersonScopeTable()` | Populate the scope table (who gets bibliometrics) |
| `updateCurateSelfRole()` | Update Curator_Self role permissions |



## Components

### Setup

| File | Frequency | Purpose |
|------|-----------|---------|
| `setupReciterDB.py` | At initial setup | Runs schema SQL to create tables |
| `createDatabaseTableReciterDb.sql` | At initial setup | Creates all database tables |
| `createEventsProceduresReciterDb.sql` | At initial setup | Creates all stored procedures |
| `insertBaselineDataReciterDb.sql` | At initial setup | Seed data: roles, special characters, journal rankings, NLM catalog |
| `populateAnalysisSummaryTables_v2.sql` | Reference | Standalone copy of the main nightly stored procedure |

### Update (Nightly Pipeline)

| File | Purpose |
|------|---------|
| `run_all.py` | EKS orchestrator: runs all pipeline steps in sequence with timeout enforcement, memory logging, and S3 log upload |
| `retrieveArticles.py` | Fetches person and article data from S3 and DynamoDB in batches |
| `retrieveNIH.py` | Fetches NIH iCite metrics in batches of 150; loads to staging table with validation and atomic swap |
| `retrieveAltmetric.py` | Fetches Altmetric scores for articles published in the last 2 years |
| `updateReciterDB.py` | Bulk data loader using `LOAD DATA LOCAL INFILE` with retry and reconnect logic |
| `dataTransformer.py` | Transforms ReCiter JSON output to CSV format for all `person_*` tables |
| `run_nightly_indexing.sh` | Calls `populateAnalysisSummaryTables_v2()` with progress monitoring, auto-retry, and auto-restore |
| `abstractImport.py` | Imports PubMed abstracts from DynamoDB (parallel batch fetches) |
| `conflictsImport.py` | Imports conflict-of-interest statements from DynamoDB |
| `executeFeatureGenerator.py` | Triggers ReCiter feature generator API with rate limiting and metrics |



## Operations

### Common Tasks

**Run the stored procedure manually** (rebuilds all summary tables, ~20 minutes):
```sql
CALL populateAnalysisSummaryTables_v2();
```

**Monitor progress:**
```sql
SELECT step, substep, status, rows_affected, created_at
FROM analysis_job_log
WHERE job_id = (SELECT MAX(job_id) FROM analysis_job_log)
ORDER BY id DESC LIMIT 10;
```

**Check table health:**
```sql
-- Row counts
SELECT 'analysis_summary_article' t, COUNT(*) n FROM analysis_summary_article
UNION ALL SELECT 'analysis_summary_person', COUNT(*) FROM analysis_summary_person
UNION ALL SELECT 'analysis_nih', COUNT(*) FROM analysis_nih;

-- Check for duplicates in analysis_nih (total should equal uniq)
SELECT COUNT(*) AS total, COUNT(DISTINCT pmid) AS uniq FROM analysis_nih;
```

**Restore from backup** (if the SP fails mid-swap):
```sql
CALL restore_from_backup_v2();
```

**View CronJob status:**
```bash
kubectl -n reciter get cronjob reciterdb
kubectl -n reciter get jobs --sort-by=.metadata.creationTimestamp | tail -5
kubectl -n reciter logs job/<job-name> 2>&1 | tail -50
```


### Troubleshooting

**"Bibliometric Analysis" page is blank in Publication Manager:**
The UI reads from `analysis_summary_article`. If NIH columns are NULL, the stored procedure needs to be re-run after `analysis_nih` is refreshed:
```sql
CALL populateAnalysisSummaryTables_v2();
```

**Validation rejects NIH table swap:**
`retrieveNIH.py` compares staging row count against production. If production has duplicate pmids, validation uses `COUNT(DISTINCT pmid)` to detect this automatically. Check with:
```sql
SELECT COUNT(*) AS total, COUNT(DISTINCT pmid) AS uniq FROM analysis_nih;
```

**Stored procedure appears stuck:**
The SP acquires a named lock. If a prior run crashed without releasing it:
```sql
SELECT IS_FREE_LOCK('populateAnalysisSummaryTables_v2_lock');
-- 0 = locked, 1 = free
SELECT RELEASE_LOCK('populateAnalysisSummaryTables_v2_lock');
```



## Configuration

- **Define scope of bibliometrics.** As an administrator, you have control over the people for whom the system calculates person-level bibliometrics. This allows for download of a person's bibliometric analysis complete with comparisons to institutional peers. To do this, update the `populateAnalysisSummaryPersonScopeTable` stored procedure which populates the `analysis_summary_person_scope` table. Here at Weill Cornell Medicine, we consider only full-time employed faculty (i.e., `person_person_type.personType = academic-faculty-weillfulltime`).
- **Importing additional journal-level metrics (optional).** ReCiterDB ships with journal impact data from Scimago Journal Rank. If you have another journal level impact metric, which uses ISSN as a primary key, it can be imported into the `journal_impact_alternative` table.


## More on the ReCiter suite of applications

As the figure describes, the ReCiter suite of applications can fully manage many key steps in institutional publication management.

<img src="https://github.com/wcmc-its/ReCiterDB/blob/master/files/howReciterWorks.png" width=800 />


| Repository | Required? | Purpose |
|------------|-----------|---------|
| [ReCiter](https://github.com/wcmc-its/ReCiter) | Yes | Core ML scoring engine: stores identity info, retrieves articles from PubMed/Scopus, estimates authorship likelihood, shares data through web services |
| [ReCiter PubMed Retrieval Tool](https://github.com/wcmc-its/ReCiter-PubMed-Retrieval-Tool) | Yes | Retrieve and normalize publication data from PubMed |
| [ReCiter Scopus Retrieval Tool](https://github.com/wcmc-its/ReCiter-Scopus-Retrieval-Tool) | No | Retrieve and normalize publication data from Scopus |
| [ReCiter Publication Manager](https://github.com/wcmc-its/ReCiter-Publication-Manager) | No | Web UI for curating publications and generating bibliometric reports |
| [ReCiterDB](https://github.com/wcmc-its/ReCiterDB) | For Publication Manager | Scripts for retrieving data from ReCiter and a relational database for publication and bibliometric data |
| [ReCiter-CDK](https://github.com/wcmc-its/ReCiter-CDK) | No | AWS CDK infrastructure-as-code for deploying the entire ReCiter ecosystem |
