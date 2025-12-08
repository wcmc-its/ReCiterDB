FROM python:3.11-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY init.py ./

ENV PYTHONUNBUFFERED=1 


## Retrieve data from ReCiter and import into ReCiterDB

COPY update/retrieveNIH.py ./
COPY update/retrieveDynamoDb.py ./
COPY update/retrieveS3.py ./
COPY update/retrieveAltmetric.py ./
COPY update/updateReciterDB.py ./
COPY update/abstractImport.py ./
COPY update/conflictsImport.py ./
COPY update/executeFeatureGenerator.py ./


## Shell script for running the stored procedure
COPY update/run_nightly_indexing.sh ./
RUN chmod +x run_nightly_indexing.sh


## Make directories

RUN pip3 install --no-cache-dir -r requirements.txt
RUN mkdir -p temp
RUN mkdir -p temp/parsedOutput
RUN mkdir -p temp/s3Output


## Run imports then the indexing SP
CMD [ "/bin/bash", "-c", "python3 executeFeatureGenerator.py && python3 ./retrieveS3.py && python3 ./retrieveDynamoDb.py && python3 ./updateReciterDB.py && python3 ./retrieveNIH.py && python3   ./conflictsImport.py && python3 ./abstractImport.py && ./run_nightly_indexing.sh" ]  
