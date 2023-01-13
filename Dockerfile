FROM python:3.9-slim

WORKDIR /usr/src/app

COPY requirements.txt ./
COPY init.py ./

ENV PYTHONUNBUFFERED=1 

## Retrieve data from ReCiter and import into ReCiterDB

COPY update/retrieveNIH.py ./
# COPY update/retrieveDynamoDb.py ./
COPY update/retrieveS3.py ./

## Make directories

RUN pip3 install --no-cache-dir -r requirements.txt
RUN mkdir -p temp
RUN mkdir -p temp/parsedOutput
RUN mkdir -p temp/s3Output


# RUN cut -d: -f1 /etc/passwd

# ARG GetMyUsername
# RUN echo ${GetMyUsername}

# RUN chown -R root:root /usr/src/app/temp
# RUN chmod -R 777 temp


## Update

CMD [ "/bin/bash", "-c", "python3 ./retrieveS3.py && python3 ./retrieveNIH.py" ]

