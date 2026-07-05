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
COPY update/run_all.py ./

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
