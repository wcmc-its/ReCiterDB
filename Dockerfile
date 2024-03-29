
FROM python:3.11-slim

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*
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

COPY update/feedbackScoreArticlesUpdateDatabase.py ./
COPY update/feedbackScoringModel.keras ./
COPY update/scaler.save ./
COPY update/scoring.py ./

RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*

## Make directories

RUN pip install --no-cache-dir -r requirements.txt
## RUN pip3 install pandas
## RUN pip3 install keras
## RUN pip3 install sklearn
## RUN pip3 install sqlalchemy
## RUN pip3 install joblib
RUN mkdir -p temp
RUN mkdir -p temp/parsedOutput
RUN mkdir -p temp/s3Output


## Update

# CMD [ "/bin/bash", "-c", "python3 ./retrieveDynamoDb.py && python3 ./retrieveS3.py && python3 ./updateReciterDB.py && python3 ./retrieveNIH.py"  ]

CMD [ "/bin/bash", "-c", "python3 executeFeatureGenerator.py && python3 ./retrieveS3.py && python3 ./retrieveDynamoDb.py && python3 ./updateReciterDB.py && python3 ./retrieveNIH.py && python3 ./conflictsImport.py && python3 ./abstractImport.py"  ]
