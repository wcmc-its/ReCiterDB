FROM python:3.6-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY init.py ./

ENV PYTHONUNBUFFERED=1 

## Retrieve data from ReCiter and import into ReCiterDB

COPY update/retrieveNIH.py ./
COPY update/retrieveDynamoDb.py ./


## Make directories

RUN pip3 install --no-cache-dir -r requirements.txt
RUN mkdir -p temp
RUN mkdir -p temp/parsedOutput
RUN mkdir -p temp/s3Output


## Update

CMD [ "/bin/bash", "-c", "python3 ./retrieveDynamoDb.py && python3 ./retrieveNIH.py" ]

