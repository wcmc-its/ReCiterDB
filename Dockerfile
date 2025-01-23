FROM python:3.12-slim

WORKDIR /usr/src/app

# Install required system packages
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*

# Copy application files and requirements
COPY requirements.txt ./
COPY init.py ./

ENV PYTHONUNBUFFERED=1 

# Copy additional Python scripts
COPY update/abstractImport.py ./
COPY update/conflictsImport.py ./
COPY update/dataTransformer.py ./
COPY update/executeFeatureGenerator.py ./
COPY update/retrieveAltmetric.py ./
COPY update/retrieveArticles.py ./
COPY update/retrieveNIH.py ./
COPY update/updateReciterDB.py ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt
# Or, if not using requirements.txt:
# RUN pip install requests boto3 dynamodb-json

# Create required directories
RUN mkdir -p temp
RUN mkdir -p temp/parsedOutput
RUN mkdir -p temp/s3Output

# Final command
CMD [ "/bin/bash", "-c", "python3 executeFeatureGenerator.py && python3 ./retrieveArticles.py && python3 ./retrieveNIH.py && python3 ./conflictsImport.py && python3 ./abstractImport.py" ]
