FROM python:3.6-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY init.py ./


## Database, tables, stored procedures, and events. 

COPY setup/init.py setup/init.py 
COPY setup/createDatabaseTableReciterDb.sql setup/createDatabaseTableReciterDb.sql 
COPY setup/createEventsProceduresReciterDb.sql setup/createEventsProceduresReciterDb.sql 
COPY setup/insertBaselineDataReciterDb.sql setup/insertBaselineDataReciterDb.sql 
COPY setup/setupReciterDB.py setupReciterDB.py 


## Retrieve data from ReCiter and import into ReCiterDB

COPY update/init.py update/init.py 
COPY update/retrieveAltmetric.py update/retrieveAltmetric.py 
COPY update/retrieveDynamoDb.py update/retrieveDynamoDb.py 
COPY update/retrieveNIH.py update/retrieveNIH.py 
COPY update/updateReciterDB.py update/updateReciterDB.py 
COPY update/retrieveS3.py update/retrieveS3.py 
COPY update/retrieveUpdate.sh retrieveUpdate.sh 


RUN pip3 install --no-cache-dir -r requirements.txt


## Create necessary directories

RUN mkdir -p update/temp
RUN mkdir -p update/temp/parsedOutput
RUN mkdir -p update/temp/s3Output


## Set permissions on shell script

RUN chmod a+x retrieveUpdate.sh


## Setup

## CMD [ "/bin/bash", "-c", "python3 ./setup/setupReciterDB.py" ]


## Update

CMD [ "/bin/bash", "-c", "./retrieveUpdate.sh" ]

## && python3 ./retrieveDynamoDb.py && python3 ./retrieveS3.py && python3 ./updateReciterDB.py && python3 ./retrieveNIH.py

