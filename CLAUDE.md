# ReCiterDB — Agent Guide

## SECURITY: No Plaintext Credentials

**NEVER commit passwords, API keys, or secrets to this repo.** All credentials are managed through:

- **Kubernetes Secrets** (`kubernetes/k8-secrets.yaml`) — contains base64-encoded `DB_USERNAME`, `DB_PASSWORD`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`. The file in this repo is a **template** with placeholders (`<<DB_PASSWORD>>`). Actual secrets live in the EKS cluster.
- **Environment variables** — all scripts read credentials from env vars at runtime, never from files.
- **AWS Secrets Manager** — Docker Hub credentials for builds are fetched from Secrets Manager in `k8-buildspec.yml`.

To connect to the database locally, export env vars in your terminal session. Do not create `.env` files or hardcode connection strings.

---

## What This Project Is

ReCiterDB is a MariaDB database + Python/shell ETL pipeline that:

1. Pulls publication data from ReCiter (S3/DynamoDB), NIH iCite, and Altmetric
2. Stores it in a relational schema (~30 tables)
3. Runs a stored procedure nightly to compute bibliometric summary tables (h-index, percentiles, citation counts)
4. Serves as the backend for [ReCiter Publication Manager](https://github.com/wcmc-its/ReCiter-Publication-Manager)

**Runtime**: Python 3.12, MariaDB (not MySQL — MariaDB-specific SQL features are used)
**Deployment**: Docker container on AWS EKS as a CronJob

---

## Related Projects

All repos are under `https://github.com/wcmc-its/`:

| Repo | Purpose | Relationship to ReCiterDB |
|------|---------|---------------------------|
| [ReCiter](https://github.com/wcmc-its/ReCiter) | Core ML scoring engine (Java) | Upstream data source — writes to S3/DynamoDB |
| [ReCiter-Publication-Manager](https://github.com/wcmc-its/ReCiter-Publication-Manager) | Web UI (Next.js/React) | Downstream consumer — queries `analysis_summary_*` tables |
| [ReCiter-PubMed-Retrieval-Tool](https://github.com/wcmc-its/ReCiter-PubMed-Retrieval-Tool) | PubMed API wrapper (Java) | Required by ReCiter core |
| [ReCiter-Scopus-Retrieval-Tool](https://github.com/wcmc-its/ReCiter-Scopus-Retrieval-Tool) | Scopus API wrapper (Java) | Optional, improves accuracy |
| [ReCiter-CDK](https://github.com/wcmc-its/ReCiter-CDK) | AWS infrastructure-as-code | Deploys this project + all dependencies |

Local reference folders (not in git):
- `/Users/paulalbert/Dropbox/Index/ReCiter/ReCiter - Indexing Nightly Database Job/` — working copies, scratch SQL, CSV snapshots
- `/Users/paulalbert/Dropbox/Index/ReCiter/Publication Manager - Documentation/` — UI mockups and feature specs

---

## Branches

| Branch | Purpose |
|--------|---------|
| `master` | Production — CronJob builds from this |
| `dev` | Development — PRs merge here first, then to master |

CodeBuild only builds Docker images on `master` and `dev` pushes.

---

## Directory Structure

```
ReCiterDB/
├── setup/                                         # Database initialization
│   ├── setupReciterDB.py                          # Orchestrator: runs schema SQL
│   ├── createDatabaseTableReciterDb.sql           # All table DDL
│   ├── createEventsProceduresReciterDb.sql        # All stored procedures (v2 + report generation)
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
│   └── k8-secrets.yaml                            # Secret TEMPLATE (not real secrets)
│
├── Dockerfile                                     # Python 3.12-slim container
├── buildspec.yml                                  # CodeBuild → ECR
├── k8-buildspec.yml                               # CodeBuild → EKS image update
├── requirements.txt                               # Python dependencies
└── README.md                                      # Public-facing documentation
```

### Files NOT in the Docker image (legacy/unused)

| File | Status | Notes |
|------|--------|-------|
| `update/retrieveUpdate.sh` | Legacy | Replaced by `run_all.py` |
| `update/retrieveS3.py` | Legacy | Replaced by `retrieveArticles.py` |
| `update/retrieveDynamoDb.py` | Legacy | Replaced by `retrieveArticles.py` |
| `update/scoring.py` | Unused | ML feedback scoring experiment, never deployed |
| `update/feedbackScoreArticlesUpdateDatabase.py` | Unused | Companion to scoring.py |
| `update/feedbackScoringModel.keras` | Unused | Keras model for scoring.py |
| `update/scaler.save` | Unused | Scaler for scoring.py |
| `update/analysis_nih*.csv` | Ephemeral | Data snapshots from last run, regenerated nightly |
| `update/app.log` | Ephemeral | Log from last local run |
| `setup/check_analysis_summary_status.sql` | Empty | Placeholder, never implemented as standalone file |

---

## Nightly Pipeline Flow

The CronJob runs daily. Entry point is `run_all.py`:

```
CronJob (17:30 UTC daily, runs on Ec2Spot nodes)
 └─ run_all.py
     ├─ 1. executeFeatureGenerator.py    Trigger ReCiter ML scoring API
     ├─ 2. retrieveArticles.py           Pull person/article from S3 + DynamoDB
     ├─ 3. retrieveNIH.py                NIH iCite API → analysis_nih (atomic swap)
     ├─ 4. run_nightly_indexing.sh        Run populateAnalysisSummaryTables_v2()
     │      └─ Polls analysis_job_log every 3s for progress
     │      └─ Auto-retries 3x with 60s backoff
     │      └─ Auto-restores from backup on failure
     ├─ 5. abstractImport.py             PubMed abstracts from DynamoDB
     └─ 6. conflictsImport.py            COI statements from DynamoDB
```

Total runtime: ~95 minutes for ~270K articles, ~2,500 people.

### Key patterns

- **Atomic table swap** (used by `retrieveNIH.py` and the stored procedure): Load into `table_new`, validate row counts, then `RENAME TABLE table TO table_backup, table_new TO table`. Zero downtime.
- **Validation gate**: `retrieveNIH.py` requires staging table to have >= 80% of production row count (using unique pmid count to handle corruption). Prevents bad data from reaching production.
- **UNIQUE constraint**: `analysis_nih.pmid` has a UNIQUE KEY to prevent duplicate loading (added March 2026 after a duplication incident).

---

## Database Schema

### Tables that matter most

**Publication Manager reads from these** (populated by the nightly SP):
- `analysis_summary_article` — one row per pmid: title, journal, citations, NIH percentile, RCR
- `analysis_summary_author` — one row per person+pmid: author list string, author position
- `analysis_summary_author_list` — one row per author+pmid: individual author name + rank
- `analysis_summary_person` — one row per person: h-index, article counts, percentile rankings

**Data source tables** (populated by ETL scripts):
- `person_article` — ReCiter scoring output (~80 columns of evidence per person+pmid pair)
- `person_article_author` — author list with rank, targetAuthor flag, ORCID, equalContrib
- `analysis_nih` — NIH iCite metrics (RCR, percentile, citation count, field citation rate)
- `analysis_nih_cites` / `analysis_nih_cites_clin` — citation network (multi-row per pmid, no unique constraint)
- `analysis_altmetric` — Altmetric social metrics

**Admin tables** (managed by Publication Manager UI):
- `admin_users`, `admin_roles`, `admin_users_roles` — RBAC
- `admin_feedback_log` — user curation actions
- `admin_settings` — UI configuration (JSON)
- `analysis_summary_person_scope` — defines who gets bibliometric analysis
- `analysis_override_author_position` — manual author position corrections

**Reference data** (loaded once at setup):
- `journal_impact_scimago`, `journal_impact_alternative` — journal rankings
- `journal_nlm` — NLM catalog
- `analysis_special_characters` — 294 Unicode → RTF mappings

### Key constraints

- `analysis_nih.pmid` has a **UNIQUE KEY** (prevents duplicate row loading)
- `analysis_nih_cites` and `analysis_nih_cites_clin` do **NOT** have unique constraints (legitimately many rows per pmid)
- `person_article` is keyed on `(personIdentifier, pmid)` via index

---

## Stored Procedures

All defined in `setup/createEventsProceduresReciterDb.sql`. The main nightly SP also has a standalone copy at `setup/populateAnalysisSummaryTables_v2.sql`.

### Nightly indexing

| Procedure | Purpose |
|-----------|---------|
| `populateAnalysisSummaryTables_v2()` | Main job: rebuild all `analysis_summary_*` tables with atomic swap |
| `setup_job_logging()` | Create/upgrade `analysis_job_log` table |
| `log_progress()` | Insert progress entry to `analysis_job_log` |
| `cleanup_staging_tables_v2()` | Drop `*_new` staging tables on failure |
| `restore_from_backup_v2()` | Rename `*_backup` tables back to production |
| `check_analysis_summary_status()` | Show row counts and backup table status |
| `view_job_progress(job_id)` | Show all log entries for a job |
| `get_latest_progress()` | Show most recent log entry |

### Report generation

| Procedure | Purpose |
|-----------|---------|
| `generateBibliometricReport(personID)` | Full bibliometric report for one person |
| `generatePubsRTF()` | RTF publication list (all) |
| `generatePubsPeopleOnlyRTF()` | RTF publication list (people only) |
| `generatePubsNoPeopleRTF()` | RTF publication list (no people) |
| `generateEmailNotifications()` | New publication email alerts |

### Admin

| Procedure | Purpose |
|-----------|---------|
| `populateAnalysisSummaryPersonScopeTable()` | Populate the scope table (who gets bibliometrics) |
| `updateCurateSelfRole()` | Update Curator_Self permissions |

---

## Environment Variables

| Variable | Purpose | Source |
|----------|---------|--------|
| `DB_HOST` | MariaDB hostname | K8s ConfigMap |
| `DB_USERNAME` | Database user | K8s Secret |
| `DB_PASSWORD` | Database password | K8s Secret |
| `DB_NAME` | Database name (`reciterdb`) | K8s ConfigMap |
| `AWS_ACCESS_KEY_ID` | For S3/DynamoDB access | K8s Secret |
| `AWS_SECRET_ACCESS_KEY` | For S3/DynamoDB access | K8s Secret |
| `AWS_DEFAULT_REGION` | AWS region (`us-east-1`) | K8s ConfigMap |
| `URL` | ReCiter feature generator endpoint | K8s ConfigMap |
| `API_KEY` | ReCiter API auth token | K8s Secret |
| `LOG_FILE` | Log file path | K8s ConfigMap |
| `S3_BUCKET` | S3 bucket for log archival | K8s ConfigMap |
| `S3_KEY_PREFIX` | S3 key prefix for logs | K8s ConfigMap |
| `SCRIPT_TIMEOUT_SECONDS` | Pipeline timeout (default: 15000s) | K8s ConfigMap |

---

## Common Operations

### Run the nightly SP manually

```bash
mysql -h $DB_HOST -u $DB_USERNAME -p"$DB_PASSWORD" $DB_NAME \
  -e "CALL populateAnalysisSummaryTables_v2();"
```

This takes ~20 minutes. Monitor progress:

```bash
mysql ... -e "SELECT step, substep, status, rows_affected, created_at
FROM analysis_job_log
WHERE job_id = (SELECT MAX(job_id) FROM analysis_job_log)
ORDER BY id DESC LIMIT 10;"
```

### Check table health

```sql
-- Row counts for key tables
SELECT 'analysis_summary_article' t, COUNT(*) n FROM analysis_summary_article
UNION ALL SELECT 'analysis_summary_person', COUNT(*) FROM analysis_summary_person
UNION ALL SELECT 'analysis_nih', COUNT(*) FROM analysis_nih;

-- Check for duplicate pmids in analysis_nih (should show total = uniq)
SELECT COUNT(*) as total, COUNT(DISTINCT pmid) as uniq FROM analysis_nih;
```

### Restore from backup (if SP fails mid-swap)

```bash
mysql ... -e "CALL restore_from_backup_v2();"
```

### View CronJob status

```bash
kubectl -n reciter get cronjob reciterdb
kubectl -n reciter get jobs --sort-by=.metadata.creationTimestamp | tail -5
kubectl -n reciter logs job/<job-name> 2>&1 | tail -50
```

---

## Troubleshooting

### "Bibliometric Analysis" page is blank
The Publication Manager reads from `analysis_summary_article`. If NIH columns (`citationCountNIH`, `percentileNIH`, `relativeCitationRatioNIH`) are NULL, the SP needs to be re-run after `analysis_nih` is refreshed:
```sql
CALL populateAnalysisSummaryTables_v2();
```

### Validation rejects NIH table swap
`retrieveNIH.py` compares staging row count against production. If production has duplicate pmids (corruption), the validation uses `COUNT(DISTINCT pmid)` instead of `COUNT(*)`. Check:
```sql
SELECT COUNT(*) as total, COUNT(DISTINCT pmid) as uniq FROM analysis_nih;
```
If `total != uniq`, the next run will auto-detect and self-heal.

### SP appears stuck
Check `analysis_job_log` for the last entry. The SP acquires a named lock (`populateAnalysisSummaryTables_v2_lock`). If a prior run crashed without releasing it:
```sql
SELECT IS_FREE_LOCK('populateAnalysisSummaryTables_v2_lock');
-- 0 = locked, 1 = free
SELECT RELEASE_LOCK('populateAnalysisSummaryTables_v2_lock');
```

### Pipeline timeout
Default timeout is 15,000 seconds (~4.2 hours). If the pipeline consistently times out, check:
1. NIH iCite API responsiveness (batch of 150, 1-sec delay each)
2. DynamoDB scan throughput
3. Database query performance (large JOINs in the SP)

---

## Architectural Decisions

1. **MariaDB over MySQL**: The stored procedures use MariaDB-specific features (e.g., `REGEXP_REPLACE` with `BINARY` modifier, window functions in older versions).
2. **Atomic table swap over in-place UPDATE**: Prevents read inconsistency during nightly rebuilds. Production tables are always fully consistent.
3. **CSV + LOAD DATA over INSERT**: Bulk loading via `LOAD DATA LOCAL INFILE` is 10-100x faster than row-by-row inserts for tables with 250K+ rows.
4. **Set-based h-index**: Uses `ROW_NUMBER()` window function instead of loop-based computation. Runs in seconds vs. minutes.
5. **Fibonacci retry for NIH API**: Gracefully handles rate limiting without overwhelming the API.
6. **Spot instances**: CronJob runs on Ec2Spot for cost savings. Acceptable because the job is idempotent and can be retried.

---

## Incident History

### Dec 18, 2025 — analysis_nih duplicate loading
**What happened**: Manual reruns of the CronJob loaded `analysis_nih` with 527K rows (2x the correct ~264K) because `pmid` had no UNIQUE constraint.
**Impact**: Every nightly run for 77 days failed validation (267K/527K = 50.7% < 80% threshold). The `analysis_nih`, `analysis_nih_cites`, and `analysis_nih_cites_clin` tables were stale.
**Fix** (March 5, 2026):
- Added UNIQUE KEY on `analysis_nih.pmid` (schema + runtime)
- `validate_data()` now detects duplicates and uses unique count for comparison
- Manually deduped production table via `CREATE TABLE ... SELECT MIN(id) ... GROUP BY pmid` + atomic swap
- PRs: [#71](https://github.com/wcmc-its/ReCiterDB/pull/71) (master), [#72](https://github.com/wcmc-its/ReCiterDB/pull/72) (dev)
**Lesson**: Always add UNIQUE constraints on columns that should be unique. The safety check (validation gate) can become the problem if production data is corrupt.
