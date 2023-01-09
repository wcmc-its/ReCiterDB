FROM python:3.6-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY init.py ./


## Database, tables, stored procedures, and events. 

COPY /setup/setupReCiterDb.py ./

COPY /setup/createDatabaseTableReciterDb.sql ./
COPY /setup/createEventsProceduresReciterDb.sql ./
COPY /setup/insertBaselineDataReciterDb.sql ./


## Retrieve data from ReCiter and import into ReCiterDB

COPY /update/updateReciterDB.py ./

COPY /update/retrieveS3.py ./
COPY /update/retrieveDynamoDb.py ./
COPY /update/retrieveNIH.py ./
# COPY /update/retrieveAltmetric.py



RUN pip3 install --no-cache-dir -r requirements.txt
RUN mkdir -p ReCiter
RUN mkdir -p AnalysisOutput

RUN chmod a+x /update/importIntoReCiterDB.sh

CMD [ "/bin/bash", "-c", "python3 ./setup/setupReCiterDb.py && python3 /update/updateReciterDB.py" ]
