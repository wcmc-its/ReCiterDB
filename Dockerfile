FROM python:3.12-slim

WORKDIR /usr/src/app

# Install required system packages
RUN apt-get update && apt-get install -y --no-install-recommends default-mysql-client libmariadb-dev gcc && rm -rf /var/lib/apt/lists/*

# Copy application files and requirements
COPY requirements.txt ./
COPY init.py ./

ENV PYTHONUNBUFFERED=1 

# Copy additional Python scripts
COPY update/retrieveNIH.py ./
COPY update/retrieveReporter.py ./
COPY update/retrieveAltmetric.py ./
COPY update/retrieveArticles.py ./
COPY update/updateReciterDB.py ./
COPY update/abstractImport.py ./
COPY update/conflictsImport.py ./
COPY update/dataTransformer.py ./
COPY update/executeFeatureGenerator.py ./
COPY update/retrieveExternalArticles.py ./
COPY update/run_all.py ./

# Nightly identity build from ED (LDAP) + ASMS (MSSQL). Replaces the Splunk
# saved search "reciter identity update". Own CronJob (k8-cronjob-identity.yaml),
# runs at 12:00 UTC ahead of the reciterdb job -- deliberately NOT in run_all.py.
COPY update/buildIdentity.py ./

# AAR Scopus lane (not-in-PubMed WCM authorship detector — weekly, gated in run_all.py)
COPY update/identity_index.py ./
COPY update/aar_db.py ./
COPY update/aar_universe_scopus.py ./
COPY update/scopus_afids.csv ./

# AAR PubMed lane (orphan-authorship detector + IO/FB scoring — weekly, gated in run_all.py)
COPY update/aar_universe.py ./
COPY update/aar_gate.py ./
COPY update/aar_matcher.py ./
COPY update/adversarial_attribution_review.py ./
COPY update/aar_orchestrator.py ./
COPY update/preprocessing.py ./
COPY update/aar_models/ ./aar_models/
COPY update/aar_data/ ./aar_data/

# AAR nightly closer: dismiss open rows ReCiter already attributed (#186, every night,
# gated in run_all.py). aar_report_changed_picks.py is required at runtime by
# aar_dismiss_byline_owner.py's own module-level import (for _cwid_eq) -- it cannot be
# made lazy there, unlike aar_reconcile_open.py's own CLASS-B imports (T2).
COPY update/aar_report_changed_picks.py ./
COPY update/aar_reconcile_open.py ./
COPY update/aar_dismiss_byline_owner.py ./
# aar_reconcile_open.py's CLASS B imports aar_sweep_stale for NULL_COLUMNS / _SELECT_COLS /
# _snapshot. That import is lazy (_load_class_b_modules), so CLASS A runs fine without it and
# the nightly closer never noticed it was absent -- but every CLASS B invocation from this
# image died on ModuleNotFoundError. Its own deps (aar_db, identity_index, aar_universe,
# aar_universe_scopus) are all already copied above.
COPY update/aar_sweep_stale.py ./

# Manual reference tool: per-document backfill of producer-owned authorship_review
# columns (authors_json / issn / isbn) on rows a sweep can no longer revisit. Not run by
# run_all.py -- invoked by hand as a one-off Job off this image. Its own docstring says it
# "must run inside the reciterdb container", which was not true until it was shipped here:
# it sys.path.insert("/usr/src/app") and imports aar_universe_scopus + aar_db, both of
# which are already in the image above.
COPY update/targeted_authors_backfill.py ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
# Or, if not using requirements.txt:
# RUN pip install requests boto3 dynamodb-json


## Shell script for running the stored procedure
COPY update/run_nightly_indexing.sh ./
RUN chmod +x run_nightly_indexing.sh

# Create required directories
RUN mkdir -p temp
RUN mkdir -p temp/parsedOutput
RUN mkdir -p temp/s3Output


## Run imports then the indexing SP
CMD [ "/bin/bash", "-c", "python3 run_all.py"] 
